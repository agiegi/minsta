from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    Header,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, Response
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
import base64
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


# ✨ PWAアイコン: AndroidのChromeはホーム画面インストールの要件として
# 「実URLで配信されるPNGアイコン(192pxと512px)」を要求する。
# 従来のdata URI SVG絵文字アイコンはこの要件を満たさず、Androidで
# ホーム画面に追加できない原因だった。森の大樹から生成したPNGを
# base64で埋め込み、ルートとして配信する。
ICON_PNG_192_B64 = "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"
ICON_PNG_512_B64 = "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"


@app.get("/icon-192.png")
def get_icon_192():
    return Response(
        content=base64.b64decode(ICON_PNG_192_B64), media_type="image/png"
    )


@app.get("/icon-512.png")
def get_icon_512():
    return Response(
        content=base64.b64decode(ICON_PNG_512_B64), media_type="image/png"
    )


@app.get("/manifest.json")
def get_manifest():
    return JSONResponse(
        {
            "name": "みんスタ - みんなで育てる学習の森",
            "short_name": "みんスタ",
            "id": "/",
            "scope": "/",
            "start_url": "/",
            "display": "standalone",
            "background_color": "#FFFBF0",
            "theme_color": "#66BB6A",
            "icons": [
                {
                    "src": "/icon-192.png",
                    "sizes": "192x192",
                    "type": "image/png",
                    "purpose": "any",
                },
                {
                    "src": "/icon-512.png",
                    "sizes": "512x512",
                    "type": "image/png",
                    "purpose": "any",
                },
                {
                    "src": "/icon-512.png",
                    "sizes": "512x512",
                    "type": "image/png",
                    "purpose": "maskable",
                },
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