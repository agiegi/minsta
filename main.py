from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    Header,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    func,
    text,
    inspect,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv
import urllib.request
import urllib.parse
import urllib.error
import secrets
import random
import bcrypt
import asyncio
import json
import os

# .env ファイルから環境変数を読み込む。
# APIキーなどの秘密情報はコードに直接書かず、ここから取得する。
load_dotenv()

# Google Books API キー。.env に「GOOGLE_BOOKS_API_KEY=...」の形式で設定する。
# 値はサーバー側にのみ存在し、ブラウザには一切渡らない。
GOOGLE_BOOKS_API_KEY = os.environ.get("GOOGLE_BOOKS_API_KEY", "")


# --- 時刻ユーティリティ ---
# DB には従来どおり「タイムゾーン情報を持たない UTC（naive UTC）」を保存する。
# datetime.utcnow() は Python 3.12 以降で非推奨のため、同じ値を返す関数に置き換える。
# これにより保存される値・既存データとの互換性は完全に維持される。
def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --- データベース設定 ---
# 環境変数 DATABASE_URL があればそれを使う（本番のPostgreSQL）。
# なければローカル開発用の SQLite ファイルを使う。
# これにより、手元では従来どおり SQLite、サーバーでは PostgreSQL で動作する。
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./minsta.db")

# Render の PostgreSQL URL は古い「postgres://」形式で渡されることがあるため、
# SQLAlchemy が要求する「postgresql://」形式に補正する。
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL.startswith("sqlite"):
    # SQLite はスレッド間共有のために専用オプションが必要。
    engine = create_engine(
        DATABASE_URL, connect_args={"check_same_thread": False}
    )
else:
    # PostgreSQL など。pool_pre_ping で切断済み接続を自動回復する。
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- データベースのモデル ---
# 改良: 検索に使われる外部キー列へインデックスを付与（既存データ・APIの挙動は不変、参照のみ高速化）。


class Group(Base):
    __tablename__ = "groups"
    id = Column(Integer, primary_key=True, index=True)
    goal = Column(String)


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True, autoincrement=False)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    name = Column(String)
    goal = Column(String)
    target_date = Column(String, nullable=True)
    group_id = Column(Integer, ForeignKey("groups.id"), nullable=True, index=True)
    is_ai = Column(Boolean, default=False)
    strike_count = Column(Integer, default=0)
    profile_image = Column(String, nullable=True)
    # ✨ 門出（卒業）を一度でも経験したか。桜バッジの表示に使う。
    has_graduated = Column(Boolean, default=False)
    # ✨ 称号バッジ（運営が手動で付与）。開発者=大樹 / アドバイザー=雫 / テスター=双葉
    is_developer = Column(Boolean, default=False)
    is_advisor = Column(Boolean, default=False)
    is_tester = Column(Boolean, default=False)
    # ✨ 認証用トークン。ログイン/登録時に発行し、リクエストの本人確認に使う。
    auth_token = Column(String, nullable=True, index=True)


class Book(Base):
    __tablename__ = "books"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    title = Column(String)
    color = Column(String)
    cover_image = Column(String)


class Report(Base):
    __tablename__ = "reports"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    book_id = Column(Integer, ForeignKey("books.id"), nullable=True, index=True)
    content = Column(String)
    study_minutes = Column(Integer)
    reported_at = Column(DateTime, default=utcnow)


class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("groups.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    content = Column(String)
    created_at = Column(DateTime, default=utcnow)


# ✨ 新機能: 掲示板メッセージへの応援リアクション（スタンプ）
class Reaction(Base):
    __tablename__ = "reactions"
    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey("messages.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    emoji = Column(String)


Base.metadata.create_all(bind=engine)


# 既存DBへのスキーマ追補。
# create_all は「新しいテーブル」は作成するが、既存テーブルへの「列の追加」は
# 行わない。そのため、これまで運用してきたDBには認証用の auth_token 列が
# 存在しない。起動時に列の有無を調べ、なければ追加する（新規DBには影響しない）。
# inspect を使うことで SQLite / PostgreSQL のどちらでも動作する。
def ensure_schema():
    inspector = inspect(engine)
    if not inspector.has_table("users"):
        return
    cols = [c["name"] for c in inspector.get_columns("users")]
    if "auth_token" not in cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN auth_token VARCHAR"))
            conn.commit()


ensure_schema()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- 認証 ---
# ログイン/登録時にランダムなトークンを発行して User.auth_token に保存する。
# 以降のリクエストは「Authorization: Bearer <token>」ヘッダーでトークンを送り、
# サーバーはトークンからユーザーを特定する（opaque token 方式）。
# これにより、リクエストの user_id を詐称して他人のデータを操作することを防ぐ。


def issue_token() -> str:
    return secrets.token_urlsafe(32)


def get_current_user(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
) -> User:
    """Authorization ヘッダーのトークンから、ログイン中のユーザーを特定する。"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="ログインが必要です")
    token = authorization[len("Bearer "):]
    user = db.query(User).filter(User.auth_token == token).first()
    if not user:
        raise HTTPException(
            status_code=401, detail="セッションが無効です。再度ログインしてください"
        )
    return user


def require_self(current_user: User, user_id: int):
    """操作対象が本人かどうかを検証する。他人のデータなら 403 で拒否する。"""
    if current_user.id != user_id:
        raise HTTPException(
            status_code=403, detail="他のユーザーのデータは操作できません"
        )


# --- WebSocket管理マネージャー ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, group_id: int):
        await websocket.accept()
        if group_id not in self.active_connections:
            self.active_connections[group_id] = []
        self.active_connections[group_id].append(websocket)

    def disconnect(self, websocket: WebSocket, group_id: int):
        if group_id in self.active_connections:
            if websocket in self.active_connections[group_id]:
                self.active_connections[group_id].remove(websocket)
            if not self.active_connections[group_id]:
                del self.active_connections[group_id]

    async def broadcast_to_group(self, group_id: int, message: str):
        connections = self.active_connections.get(group_id)
        if not connections:
            return
        # 改良: 送信に失敗した（切断済みの）接続をその場で除去し、
        # 接続リストが無限に肥大化しないようにする。配信先・配信内容は従来と同一。
        dead: List[WebSocket] = []
        for connection in list(connections):
            try:
                await connection.send_text(message)
            except Exception:
                dead.append(connection)
        for d in dead:
            if d in connections:
                connections.remove(d)
        if group_id in self.active_connections and not self.active_connections[group_id]:
            del self.active_connections[group_id]


manager = ConnectionManager()


# --- 独自のID発行ロジック ---
def get_custom_id(db: Session, is_ai: bool):
    if is_ai:
        last_ai = (
            db.query(User).filter(User.is_ai == True).order_by(User.id.desc()).first()
        )
        if not last_ai or last_ai.id < 25:
            return 25
        return last_ai.id + 1
    else:
        last_human = (
            db.query(User).filter(User.is_ai == False).order_by(User.id.desc()).first()
        )
        if not last_human:
            return 1
        if last_human.id < 24:
            return last_human.id + 1
        elif last_human.id < 10001:
            return 10001
        else:
            return last_human.id + 1


# --- AIマッチングロジック ---
AI_NAMES = ["[AI] サクラ", "[AI] ハルト", "[AI] ミナト", "[AI] ユイ", "[AI] メンター"]

# ✨ 新機能: AIメンバーが学習報告に反応するときの応援メッセージ
AI_ENCOURAGEMENTS = [
    "ナイス学習です！その積み重ねが実を結びますよ✨",
    "今日もよく頑張りましたね🍵 しっかり休んでください",
    "コツコツ続けるその姿勢、本当に素晴らしいです🌱",
    "おつかれさまです！一緒にゴールを目指しましょう🔥",
    "今日の一歩が、未来の自分を助けてくれますよ😊",
    "継続は力なり、ですね。応援しています📣",
]

# ✨ オンボーディング: チーム参加時にAIが投稿する歓迎メッセージ。
# 登録直後の掲示板が無言だと何をすべきか分からないため、名前入りで迎えて
# 最初の行動（タイマーで学習→報告）を案内する。
AI_WELCOMES = [
    "{name}さん、ようこそ！🌱 まずは今日の学習をタイマーで記録して、報告してみてくださいね。一緒に森を育てましょう！",
    "{name}さん、はじめまして！このチームでは学習を報告し合って1本の木を育てています🌳 最初の記録、お待ちしていますね！",
    "ようこそ{name}さん！🎉 学習を報告するとチームの木が育ちます。今日の分からさっそく記録してみましょう！",
]


def adjust_group_members(db: Session, group_id: int, goal: str):
    if not group_id:
        return
    try:
        members = db.query(User).filter(User.group_id == group_id).all()
        humans = [m for m in members if not m.is_ai]
        ais = [m for m in members if m.is_ai]
        total = len(humans) + len(ais)
        while total < 3:
            used_ai_names = [a.name for a in ais]
            available_names = [n for n in AI_NAMES if n not in used_ai_names]
            if not available_names:
                available_names = AI_NAMES
            new_ai_id = get_custom_id(db, is_ai=True)
            new_ai = User(
                id=new_ai_id,
                name=random.choice(available_names),
                goal=goal,
                group_id=group_id,
                is_ai=True,
            )
            db.add(new_ai)
            db.commit()
            ais.append(new_ai)
            total += 1
        while total > 3 and ais:
            ai_to_remove = ais.pop()
            # AIを物理削除(db.delete)すると、そのAIが過去にmessages等へ
            # 投稿していた場合に外部キー制約違反(ForeignKeyViolation)で失敗し、
            # AIが抜けず4人のまま残る/登録が中途半端に成功する原因になる。
            # そのため削除せず、group_idをNULLにしてグループから外すだけにする。
            ai_to_remove.group_id = None
            total -= 1
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"adjust_group_members failed (group {group_id}): {e}")
        raise


def cleanup_floating_ais(db: Session):
    """✨ 浮いたAI（group_id=NULLのAIメンバー）を物理削除する掃除処理。

    adjust_group_membersはAIを外す際にgroup_idをNULLにするだけなので、
    放置すると浮いたAIがDBに無限に溜まっていく。AIは一度浮くと二度と
    グループへ戻らない（補充は常に新規作成）ため、いつ消しても安全。
    外部キー制約があるため reactions → messages → users の順で消す。
    （reports/booksはAIは作らないが、万一あると削除が永遠に失敗し続ける
      ため、念のため同じトランザクションで掃除する）
    戻り値: (削除したAIの数, 掲示板の表示が変わるgroup_idのset)
    """
    floating_ais = (
        db.query(User).filter(User.is_ai == True, User.group_id == None).all()
    )
    affected_groups = set()
    deleted_count = 0
    for ai in floating_ais:
        # 二重ガード: 人間は何があっても絶対に消さない
        if not ai.is_ai:
            continue
        try:
            # このAIの投稿が残っている掲示板（削除後に画面更新を流す対象）
            ai_msg_group_ids = [
                gid
                for (gid,) in db.query(Message.group_id)
                .filter(Message.user_id == ai.id)
                .distinct()
                .all()
                if gid is not None
            ]
            ai_message_ids = [
                mid
                for (mid,) in db.query(Message.id)
                .filter(Message.user_id == ai.id)
                .all()
            ]
            # 1) reactions: AIの投稿に付いたもの + AI自身が付けたもの（通常は無い）
            if ai_message_ids:
                db.query(Reaction).filter(
                    Reaction.message_id.in_(ai_message_ids)
                ).delete(synchronize_session=False)
            db.query(Reaction).filter(Reaction.user_id == ai.id).delete(
                synchronize_session=False
            )
            # 2) messages
            db.query(Message).filter(Message.user_id == ai.id).delete(
                synchronize_session=False
            )
            # 3) reports → books（AIは作らないはずだが念のため。reportsが先）
            db.query(Report).filter(Report.user_id == ai.id).delete(
                synchronize_session=False
            )
            db.query(Book).filter(Book.user_id == ai.id).delete(
                synchronize_session=False
            )
            # 4) users（本体）
            db.delete(ai)
            db.commit()
            deleted_count += 1
            affected_groups.update(ai_msg_group_ids)
        except Exception as e:
            # 1体の失敗で掃除全体を止めない（失敗分は翌晩また試行される）
            db.rollback()
            print(f"cleanup_floating_ais failed (ai {ai.id}): {e}")
    return deleted_count, affected_groups


def cleanup_ai_only_groups(db: Session):
    """✨ 人間が1人もいない「抜け殻グループ」を掃除する。

    最後の人間が卒業/キックで抜けると、adjust_group_membersがAIを
    3体に補充するため、AIだけのグループが残り続ける。夜間の人数点検は
    人間がいるグループしか見ない（無駄なAI補充をしないための仕様）ので、
    誰にも使われないGroup行とAIが永遠に溜まっていく。

    方針: グループ内のAIは group_id=NULL にして浮かせるだけにし、
    AIの物理削除は実績のある cleanup_floating_ais に任せる
    （夜間バッチでこの関数の直後に呼ばれる）。Group行は、グループ宛の
    メッセージ（過去の人間の投稿・システムメッセージ・AI応援）と
    そのリアクションを消してから削除する。
    順序: reactions → messages → AIを外す → groups。

    安全性: 1グループ=1トランザクション。掃除の最中に新規登録が
    このグループへ人間を割り当てた場合、Group削除がDBの外部キー制約で
    失敗して全体がロールバックされるため、登録側を壊すことはない
    （そのグループは翌晩、人間がいれば掃除対象外になる）。
    戻り値: (削除したグループ数, 浮かせたAIの数)
    """
    deleted_groups = 0
    detached_ais = 0
    all_groups = db.query(Group).all()
    for g in all_groups:
        members = db.query(User).filter(User.group_id == g.id).all()
        humans = [m for m in members if not m.is_ai]
        if humans:
            continue  # 人間がいるグループには絶対に触らない
        try:
            # 1) このグループ宛メッセージに付いたリアクションを消す
            msg_ids = [
                mid
                for (mid,) in db.query(Message.id)
                .filter(Message.group_id == g.id)
                .all()
            ]
            if msg_ids:
                db.query(Reaction).filter(
                    Reaction.message_id.in_(msg_ids)
                ).delete(synchronize_session=False)
            # 2) グループ宛メッセージを消す
            db.query(Message).filter(Message.group_id == g.id).delete(
                synchronize_session=False
            )
            # 3) AIメンバーを浮かせる（物理削除はcleanup_floating_aisが担当）
            ai_count = 0
            for m in members:
                if m.is_ai:
                    m.group_id = None
                    ai_count += 1
            # 4) Group行を消す（誰かが参照していればここで失敗→全ロールバック）
            db.delete(g)
            db.commit()
            deleted_groups += 1
            detached_ais += ai_count
        except Exception as e:
            # 1グループの失敗で掃除全体を止めない（翌晩また試行される）
            db.rollback()
            print(f"cleanup_ai_only_groups failed (group {g.id}): {e}")
    return deleted_groups, detached_ais


def assign_group_logic(db: Session, user: User):
    try:
        potential_groups = db.query(Group).filter(Group.goal == user.goal).all()
        target_group = None
        for g in potential_groups:
            humans = (
                db.query(User)
                .filter(User.group_id == g.id, User.is_ai == False)
                .all()
            )
            if len(humans) < 3:
                target_group = g
                break
        if not target_group:
            target_group = Group(goal=user.goal)
            db.add(target_group)
            db.commit()
            db.refresh(target_group)
        user.group_id = target_group.id
        db.commit()
        adjust_group_members(db, target_group.id, target_group.goal)
        # ✨ オンボーディング: 参加直後の掲示板に歓迎メッセージを置く。
        # 歓迎の失敗で登録そのものを失敗させないよう、ここだけで握りつぶす。
        try:
            ais = (
                db.query(User)
                .filter(User.group_id == target_group.id, User.is_ai == True)
                .all()
            )
            if ais:
                ai = random.choice(ais)
                welcome = Message(
                    group_id=target_group.id,
                    user_id=ai.id,
                    content=random.choice(AI_WELCOMES).format(name=user.name),
                )
            else:
                # AIがいない（人間3人の）チームではシステム告知として迎える
                welcome = Message(
                    group_id=target_group.id,
                    user_id=user.id,
                    content=f"【システム】{user.name}さんが森に加わりました🌱 みんなで歓迎しましょう！",
                )
            db.add(welcome)
            db.commit()
        except Exception as welcome_error:
            db.rollback()
            print(f"welcome message failed (user {user.id}): {welcome_error}")
    except Exception as e:
        db.rollback()
        print(f"assign_group_logic failed (user {user.id}): {e}")
        raise


# --- 毎晩23:59に作動するサボり点検バッチ ---
async def daily_check_task():
    print("🌿 みんスタ サボり監視バッチが正常に起動しました")
    while True:
        try:
            # トリガー判定は従来どおりサーバーのローカル時刻で行う（起動タイミングは不変）。
            now = datetime.now()
            if now.hour == 23 and now.minute == 59:
                db = SessionLocal()
                try:
                    # 改良（バグ修正）: Report.reported_at は UTC で保存されている。
                    # 従来はローカル時刻で「今日の0時」を作っていたため、サーバーのタイム
                    # ゾーン次第で当日報告の判定がずれていた。UTC基準に統一し、
                    # フロントエンド側の当日判定（toISOString = UTC基準）とも一致させる。
                    utc_now = utcnow()
                    today_start = datetime(utc_now.year, utc_now.month, utc_now.day)

                    users = (
                        db.query(User)
                        .filter(User.is_ai == False, User.group_id != None)
                        .all()
                    )
                    for u in users:
                        has_report = (
                            db.query(Report)
                            .filter(
                                Report.user_id == u.id,
                                Report.reported_at >= today_start,
                            )
                            .first()
                        )

                        if not has_report:
                            u.strike_count += 1
                            if u.strike_count >= 3:
                                old_group_id = u.group_id
                                u.group_id = None
                                u.strike_count = 0

                                msg = Message(
                                    group_id=old_group_id,
                                    user_id=u.id,
                                    content=f"【システム】{u.name}さんは3日間学習記録がなかったため、森から旅立ちました...🍂",
                                )
                                db.add(msg)
                                db.commit()

                                adjust_group_members(db, old_group_id, u.goal)
                                asyncio.create_task(
                                    manager.broadcast_to_group(old_group_id, "update")
                                )
                    db.commit()

                    # 全グループの人数点検: 人間が1人以上いるグループが
                    # 3人(人間+AI)を保てているか確認し、過不足があれば整える。
                    # サボりキックやDB直操作などで人数が狂っても、ここで自動修復される。
                    # （人間ゼロの抜け殻グループは対象外。無駄なAI補充をしないため）
                    all_groups = db.query(Group).all()
                    for g in all_groups:
                        members = (
                            db.query(User).filter(User.group_id == g.id).all()
                        )
                        human_count = len([m for m in members if not m.is_ai])
                        if human_count >= 1 and len(members) != 3:
                            try:
                                adjust_group_members(db, g.id, g.goal)
                                asyncio.create_task(
                                    manager.broadcast_to_group(g.id, "update")
                                )
                            except Exception as fix_error:
                                print(f"group {g.id} の人数調整に失敗: {fix_error}")
                    db.commit()

                    # ✨ 抜け殻グループの掃除: 人間が1人もいない（AIだけ/空の）
                    # グループは、AIを浮かせてからGroup行ごと削除する。
                    # 浮かせたAIは、直後のcleanup_floating_aisがその晩のうちに回収する。
                    try:
                        g_deleted, g_detached = cleanup_ai_only_groups(db)
                        if g_deleted:
                            print(
                                f"🧹 抜け殻グループを{g_deleted}個削除し、"
                                f"AI{g_detached}体を浮かせました"
                            )
                    except Exception as ghost_error:
                        print(f"抜け殻グループ掃除に失敗: {ghost_error}")

                    # ✨ 浮いたAIの掃除: 上の人数調整で外れた分も含めて、
                    # group_id=NULLのAIを毎晩ここで物理削除する。
                    # 失敗してもバッチ全体は止めない（翌晩リトライされる）。
                    try:
                        cleaned, touched_groups = cleanup_floating_ais(db)
                        if cleaned:
                            print(f"🧹 浮いたAIを{cleaned}体掃除しました")
                            for gid in touched_groups:
                                asyncio.create_task(
                                    manager.broadcast_to_group(gid, "update")
                                )
                    except Exception as clean_error:
                        print(f"浮いたAI掃除に失敗: {clean_error}")
                finally:
                    db.close()
                await asyncio.sleep(65)
            else:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            # アプリ終了時の正常なキャンセル。ループを抜ける。
            print("🌙 みんスタ サボり監視バッチを停止しました")
            break
        except Exception as e:
            print(f"⚠️ バッチ処理中にエラーが発生しましたが、システムを継続します: {e}")
            await asyncio.sleep(60)


# --- アプリのライフサイクル ---
# 改良: 非推奨の @app.on_event("startup") から lifespan ハンドラへ移行。
# 起動時にバッチを開始し、終了時には確実にキャンセルする（挙動は従来と同等）。
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(daily_check_task())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)

# 改良: allow_origins=["*"] と allow_credentials=True の併用はブラウザ仕様上
# 無効な組み合わせ。本アプリは Cookie 等の資格情報を用いない（同一オリジン配信）ため、
# allow_credentials=False とし、設定を仕様準拠の正しい状態にする（実挙動は不変）。
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- APIエンドポイント ---


@app.get("/")
def serve_html():
    return FileResponse("user.html")


@app.get("/manifest.json")
def get_manifest():
    return JSONResponse(
        {
            "name": "みんスタ - みんなで育てる学習の森",
            "short_name": "みんスタ",
            "start_url": "/",
            "display": "standalone",
            "background_color": "#FFFBF0",
            "theme_color": "#66BB6A",
            "icons": [
                {
                    "src": "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='0.9em' font-size='90'>🌱</text></svg>",
                    "sizes": "192x192",
                    "type": "image/svg+xml",
                }
            ],
        }
    )


@app.get("/sw.js")
def get_sw():
    return PlainTextResponse(
        "self.addEventListener('fetch', function(event) {});",
        media_type="application/javascript",
    )


@app.websocket("/ws/{group_id}")
async def websocket_endpoint(websocket: WebSocket, group_id: int):
    await manager.connect(websocket, group_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, group_id)
    except Exception:
        # 予期しない切断でも接続リストから確実に取り除く。
        manager.disconnect(websocket, group_id)


@app.post("/users/register")
def register(user_data: dict, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.email == user_data["email"]).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    new_user = None
    try:
        salt = bcrypt.gensalt()
        hashed_pw = bcrypt.hashpw(user_data["password"].encode("utf-8"), salt).decode(
            "utf-8"
        )
        new_human_id = get_custom_id(db, is_ai=False)
        new_user = User(
            id=new_human_id,
            email=user_data["email"],
            hashed_password=hashed_pw,
            name=user_data["name"],
            goal=user_data["goal"],
            target_date=user_data.get("target_date"),
            auth_token=issue_token(),
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        # チーム割り当て。ここで失敗すると、上で保存済みのユーザーだけが
        # 中途半端に残り「エラーなのに登録成功」状態になる（重複登録の原因）。
        # そのため、失敗時は保存したユーザーを削除して整合性を戻す。
        assign_group_logic(db, new_user)
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"register failed: {e}")
        # 中途半端に保存されたユーザーを後始末する
        if new_user is not None:
            try:
                saved = db.query(User).filter(User.id == new_user.id).first()
                if saved is not None:
                    db.delete(saved)
                    db.commit()
            except Exception as cleanup_error:
                db.rollback()
                print(f"register cleanup failed: {cleanup_error}")
        raise HTTPException(status_code=500, detail="Registration failed. Please retry.")
    # ✨ 認証: 発行したトークンを返す。ブラウザはこれを保存し、以降の通信に使う。
    return {
        "user": {"id": new_user.id, "name": new_user.name, "goal": new_user.goal},
        "token": new_user.auth_token,
    }


@app.post("/users/login")
def login_user(login_data: dict, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == login_data["email"]).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not bcrypt.checkpw(
        login_data["password"].encode("utf-8"), user.hashed_password.encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    # ✨ 認証: ログインのたびに新しいトークンを発行する。
    user.auth_token = issue_token()
    db.commit()
    return {
        "user": {"id": user.id, "name": user.name, "goal": user.goal},
        "token": user.auth_token,
    }


# ✨ 認証: ログアウト。サーバー側のトークンを無効化する。
@app.post("/users/logout")
def logout_user(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    current_user.auth_token = None
    db.commit()
    return {"message": "Logged out"}


# ✨ 認証: ログイン中ユーザー自身の情報を返す。
# 旧 GET /users/（全ユーザーを返す）は、他人の情報まで露出するため廃止し、
# 「自分の情報だけを返す」このエンドポイントに置き換えた。
@app.get("/users/me")
def get_me(current_user: User = Depends(get_current_user)):
    return {
        "id": current_user.id,
        "name": current_user.name,
        "goal": current_user.goal,
        "group_id": current_user.group_id,
        "target_date": current_user.target_date,
        "strike_count": current_user.strike_count,
        "profile_image": current_user.profile_image,
        "has_graduated": current_user.has_graduated,
        "is_developer": current_user.is_developer,
        "is_advisor": current_user.is_advisor,
        "is_tester": current_user.is_tester,
    }


@app.post("/users/{user_id}/goal")
async def update_goal(
    user_id: int,
    goal_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    old_group_id = user.group_id
    old_goal = user.goal
    user.goal = goal_data["goal"]
    user.target_date = goal_data.get("target_date")
    user.group_id = None
    user.strike_count = 0
    db.commit()
    if old_group_id:
        adjust_group_members(db, old_group_id, old_goal)
        await manager.broadcast_to_group(old_group_id, "update")
    assign_group_logic(db, user)
    await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Goal updated"}


@app.post("/users/{user_id}/graduate")
async def graduate(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    try:
        old_group_id = user.group_id
        # 達成の記録（桜バッジ）を立てる
        user.has_graduated = True
        # チームへ祝福の別れメッセージを残す
        if old_group_id:
            farewell = Message(
                group_id=old_group_id,
                user_id=user.id,
                content=f"🌸【システム】{user.name}さんが目標を達成し、森から巣立ちました。おめでとう！",
            )
            db.add(farewell)
        db.commit()
        # チームから外す（卒業）。残りは夜間バッチ/調整で3人に戻る。
        if old_group_id:
            user.group_id = None
            user.strike_count = 0
            db.commit()
            adjust_group_members(db, old_group_id, user.goal)
            await manager.broadcast_to_group(old_group_id, "update")
    except Exception as e:
        db.rollback()
        print(f"graduate failed (user {user_id}): {e}")
        raise HTTPException(status_code=500, detail="Graduation failed. Please retry.")
    return {"message": "Graduated", "has_graduated": True}


@app.post("/users/{user_id}/profile_image")
async def update_profile_image(
    user_id: int,
    image_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.profile_image = image_data.get("profile_image")
    db.commit()
    if user.group_id:
        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Profile image updated"}


@app.post("/users/{user_id}/name")
async def update_name(
    user_id: int,
    name_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    new_name = (name_data.get("name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="ニックネームを入力してください")
    if len(new_name) > 20:
        raise HTTPException(status_code=400, detail="ニックネームは20文字以内にしてください")
    user.name = new_name
    db.commit()
    if user.group_id:
        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Name updated", "name": user.name}


@app.get("/groups/{group_id}/members")
def get_members(
    group_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # ✨ 認証: 自分が所属するチームの情報のみ閲覧できる。
    if current_user.group_id != group_id:
        raise HTTPException(
            status_code=403, detail="このチームの情報は閲覧できません"
        )
    members = db.query(User).filter(User.group_id == group_id).all()

    # 改良（N+1クエリ解消）: 従来は人間メンバーごとに reports を個別取得していた。
    # 全メンバー分の累計学習時間を 1 クエリでまとめて集計する。返却内容は従来と同一。
    human_ids = [m.id for m in members if not m.is_ai]
    totals: Dict[int, int] = {}
    if human_ids:
        rows = (
            db.query(
                Report.user_id,
                func.coalesce(func.sum(Report.study_minutes), 0),
            )
            .filter(Report.user_id.in_(human_ids))
            .group_by(Report.user_id)
            .all()
        )
        totals = {uid: int(total or 0) for uid, total in rows}

    res = []
    for m in members:
        if m.is_ai:
            total_mins = 0
            icon = "🤖"
        else:
            total_mins = totals.get(m.id, 0)
            icon = "👤" if total_mins < 300 else "🔥"
        res.append(
            {
                "id": m.id,
                "name": m.name,
                "minutes": total_mins,
                "icon": icon,
                "profile_image": m.profile_image,
                "is_ai": m.is_ai,
                "has_graduated": m.has_graduated,
                "is_developer": m.is_developer,
                "is_advisor": m.is_advisor,
                "is_tester": m.is_tester,
            }
        )
    return res


@app.get("/users/{user_id}/stats")
def get_stats(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    # 改良: 合計の算出を DB 側の集計に委ねる（返却値は従来と同一）。
    total = (
        db.query(func.coalesce(func.sum(Report.study_minutes), 0))
        .filter(Report.user_id == user_id)
        .scalar()
    )
    return {"total_minutes": int(total or 0)}


@app.get("/users/{user_id}/books")
def get_books(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    books = db.query(Book).filter(Book.user_id == user_id).all()
    return [
        {"id": b.id, "title": b.title, "color": b.color, "cover_image": b.cover_image}
        for b in books
    ]


@app.post("/users/{user_id}/books")
def add_book(
    user_id: int,
    book_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    new_book = Book(
        user_id=user_id,
        title=book_data["title"],
        color=book_data["color"],
        cover_image=book_data["cover_image"],
    )
    db.add(new_book)
    db.commit()
    return {"message": "Book added"}


# ✨ 変更：表紙画像のアップデートも処理できるように改良
@app.post("/books/{book_id}/update")
def update_book(
    book_id: int,
    book_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    book = db.query(Book).filter(Book.id == book_id).first()
    if not book:
        return {"message": "Book already deleted"}
    # ✨ 認証: 自分の参考書だけを編集できる。
    if book.user_id != current_user.id:
        raise HTTPException(
            status_code=403, detail="他のユーザーの参考書は操作できません"
        )

    if "color" in book_data:
        book.color = book_data["color"]
    if "cover_image" in book_data and book_data["cover_image"]:
        book.cover_image = book_data["cover_image"]

    db.commit()
    return {"message": "Book updated"}


@app.delete("/books/{book_id}")
async def delete_book(
    book_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    book = db.query(Book).filter(Book.id == book_id).first()
    if not book:
        return {"message": "Book already deleted"}
    # ✨ 認証: 自分の参考書だけを削除できる。
    if book.user_id != current_user.id:
        raise HTTPException(
            status_code=403, detail="他のユーザーの参考書は操作できません"
        )
    user_id = book.user_id
    db.query(Report).filter(Report.book_id == book_id).update({"book_id": None})
    db.delete(book)
    db.commit()
    user = db.query(User).filter(User.id == user_id).first()
    if user and user.group_id:
        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Book deleted"}


@app.post("/reports/submit")
async def submit_report(
    report_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # ✨ 認証: 学習記録は必ずログイン中ユーザー本人のものとして登録する。
    # リクエストボディの user_id は信用せず、トークンから特定した本人を使う。
    user = current_user
    r = Report(
        user_id=user.id,
        book_id=report_data.get("book_id"),
        content=report_data["content"],
        study_minutes=report_data["study_minutes"],
    )
    db.add(r)
    user.strike_count = 0
    db.commit()

    if user.group_id:
        msg = Message(
            group_id=user.group_id,
            user_id=user.id,
            content=f"【学習記録】{report_data['content']} ({report_data['study_minutes']}分)",
        )
        db.add(msg)
        db.commit()

        # ✨ 新機能: グループにいるAIメンバーが、一定の確率で応援メッセージを投稿する。
        # 過疎なチームでも反応が返ってくることで「続けやすい」体験を作る。
        ai_members = (
            db.query(User)
            .filter(User.group_id == user.group_id, User.is_ai == True)
            .all()
        )
        # ✨ オンボーディング: 初回報告だけは反応を運任せにしない。
        # 「報告したら誰かが反応してくれた」という最初の体験が継続の鍵なので、
        # 必ず応援メッセージを返し、応援スタンプも付ける。
        is_first_report = (
            db.query(Report).filter(Report.user_id == user.id).count() == 1
        )
        if ai_members and (is_first_report or random.random() < 0.7):
            ai = random.choice(ai_members)
            if is_first_report:
                cheer = (
                    f"{user.name}さん、最初の学習記録おめでとうございます！🎉 "
                    f"その一歩がチームの木を育てます。一緒に頑張りましょうね🌱"
                )
            else:
                cheer = f"{user.name}さん、{random.choice(AI_ENCOURAGEMENTS)}"
            ai_msg = Message(
                group_id=user.group_id,
                user_id=ai.id,
                content=cheer,
            )
            db.add(ai_msg)
            if is_first_report:
                # 👏はフロントの応援スタンプ定番セット（STICKERS）にある絵文字
                db.add(Reaction(message_id=msg.id, user_id=ai.id, emoji="👏"))
            db.commit()

        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Report submitted"}


@app.get("/users/{user_id}/reports")
def get_reports(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_self(current_user, user_id)
    reports = (
        db.query(Report)
        .filter(Report.user_id == user_id)
        .order_by(Report.reported_at.desc())
        .all()
    )
    return [
        {
            "id": r.id,
            "book_id": r.book_id,
            "content": r.content,
            "study_minutes": r.study_minutes,
            "reported_at": r.reported_at.isoformat(),
        }
        for r in reports
    ]


@app.delete("/reports/{report_id}")
async def delete_report(
    report_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # ✨ 認証: 自分の学習記録だけを削除できる。
    # 従来は user_id をクエリで受け取っており詐称が可能だったため、
    # トークンから特定した本人の ID で絞り込む。
    report = (
        db.query(Report)
        .filter(Report.id == report_id, Report.user_id == current_user.id)
        .first()
    )
    if not report:
        return {"message": "Report already deleted"}
    db.delete(report)
    db.commit()
    if current_user.group_id:
        await manager.broadcast_to_group(current_user.group_id, "update")
    return {"message": "Report deleted"}


@app.get("/groups/{group_id}/messages")
def get_messages(
    group_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # ✨ 認証: 自分が所属するチームの掲示板のみ閲覧できる。
    if current_user.group_id != group_id:
        raise HTTPException(
            status_code=403, detail="このチームの掲示板は閲覧できません"
        )
    msgs = (
        db.query(Message)
        .filter(Message.group_id == group_id)
        .order_by(Message.created_at.asc())
        .all()
    )

    # N+1クエリ解消: 関係するユーザーを 1 クエリでまとめて取得して引き当てる。
    user_ids = {m.user_id for m in msgs}
    users_by_id: Dict[int, User] = {}
    if user_ids:
        users_by_id = {
            u.id: u for u in db.query(User).filter(User.id.in_(user_ids)).all()
        }

    # ✨ 新機能: 各メッセージへの応援リアクションを 1 クエリでまとめて取得する。
    msg_ids = [m.id for m in msgs]
    reactions_by_msg: Dict[int, list] = {}
    if msg_ids:
        for rx in db.query(Reaction).filter(Reaction.message_id.in_(msg_ids)).all():
            reactions_by_msg.setdefault(rx.message_id, []).append(rx)

    res = []
    for m in msgs:
        user = users_by_id.get(m.user_id)

        # 絵文字ごとに件数を集計し、ログイン中ユーザー自身が押したかどうかも返す。
        agg: Dict[str, dict] = {}
        for rx in reactions_by_msg.get(m.id, []):
            entry = agg.setdefault(
                rx.emoji, {"emoji": rx.emoji, "count": 0, "mine": False}
            )
            entry["count"] += 1
            if rx.user_id == current_user.id:
                entry["mine"] = True

        res.append(
            {
                "id": m.id,
                "user_name": user.name if user else "不明",
                "content": m.content,
                "profile_image": user.profile_image if user else None,
                "reactions": list(agg.values()),
            }
        )
    return res


@app.post("/groups/{group_id}/messages")
async def post_message(
    group_id: int,
    msg_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # ✨ 認証: 自分が所属するチームにのみ、本人として投稿できる。
    if current_user.group_id != group_id:
        raise HTTPException(
            status_code=403, detail="このチームには投稿できません"
        )
    msg = Message(
        group_id=group_id, user_id=current_user.id, content=msg_data["content"]
    )
    db.add(msg)
    db.commit()
    await manager.broadcast_to_group(group_id, "update")
    return {"message": "Message posted"}


# ✨ 新機能: メッセージへの応援リアクション（スタンプ）をトグルする。
@app.post("/messages/{message_id}/reactions")
async def toggle_reaction(
    message_id: int,
    data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    msg = db.query(Message).filter(Message.id == message_id).first()
    if not msg:
        raise HTTPException(status_code=404, detail="Message not found")
    # ✨ 認証: 自分が所属するチームのメッセージにのみ反応できる。
    if current_user.group_id != msg.group_id:
        raise HTTPException(
            status_code=403, detail="このメッセージには反応できません"
        )

    emoji = data["emoji"]

    # 同じユーザー・同じ絵文字が既にあれば取り消し（トグル）、なければ追加する。
    existing = (
        db.query(Reaction)
        .filter(
            Reaction.message_id == message_id,
            Reaction.user_id == current_user.id,
            Reaction.emoji == emoji,
        )
        .first()
    )
    if existing:
        db.delete(existing)
    else:
        db.add(
            Reaction(
                message_id=message_id, user_id=current_user.id, emoji=emoji
            )
        )
    db.commit()

    await manager.broadcast_to_group(msg.group_id, "update")
    return {"message": "Reaction toggled"}


# ✨ セキュリティ改良: 書籍検索のサーバー側プロキシ。
# 従来は user.html に Google Books API キーを直書きしていたため、ブラウザから
# キーが丸見えだった。検索をサーバーが代行することで、キーは .env（サーバー）に
# のみ存在し、ブラウザには一切渡らなくなる。ログイン中ユーザーのみ利用できる。
@app.get("/api/books/search")
def search_books(
    q: str,
    startIndex: int = 0,
    current_user: User = Depends(get_current_user),
):
    if not GOOGLE_BOOKS_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="書籍検索APIキーが未設定です（.env の GOOGLE_BOOKS_API_KEY を確認してください）",
        )
    params = urllib.parse.urlencode(
        {
            "q": q,
            "maxResults": 20,
            "startIndex": startIndex,
            "key": GOOGLE_BOOKS_API_KEY,
        }
    )
    url = f"https://www.googleapis.com/books/v1/volumes?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "minsta"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 429:
            raise HTTPException(
                status_code=429, detail="検索の上限に達しました。少し待ってからお試しください。"
            )
        raise HTTPException(status_code=502, detail="書籍検索に失敗しました")
    except Exception:
        raise HTTPException(status_code=502, detail="書籍検索に失敗しました")

    # フロントエンドが使用する items のみを返す。
    return {"items": data.get("items", [])}