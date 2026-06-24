from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    Header,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, Response
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
from datetime import datetime, timedelta, timezone
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


# ===== プッシュ通知(Web Push)の設定 =====
# 鍵はRenderの環境変数で設定する(VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY)。
# pywebpush未導入・鍵未設定の場合は通知機能だけが静かに無効になり、
# アプリ本体は通常どおり動く。
try:
    from pywebpush import webpush, WebPushException

    PUSH_LIB_AVAILABLE = True
except Exception:
    PUSH_LIB_AVAILABLE = False

VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_CLAIMS_EMAIL = os.getenv("VAPID_CLAIMS_EMAIL", "mailto:minsta@example.com")
PUSH_ENABLED = (
    PUSH_LIB_AVAILABLE and bool(VAPID_PUBLIC_KEY) and bool(VAPID_PRIVATE_KEY)
)
JST = timezone(timedelta(hours=9))


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
# 検索に使われる外部キー列へインデックスを付与（既存データ・APIの挙動は不変、参照のみ高速化）。


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
    # 門出（卒業）を一度でも経験したか。桜バッジの表示に使う。
    has_graduated = Column(Boolean, default=False)
    # 称号バッジ（運営が手動で付与）。開発者=大樹 / アドバイザー=雫 / テスター=双葉
    is_developer = Column(Boolean, default=False)
    is_advisor = Column(Boolean, default=False)
    is_tester = Column(Boolean, default=False)
    # 登録日時(UTC)。既存ユーザーは NULL。コホート分析に使用。
    created_at = Column(DateTime, nullable=True, default=utcnow)
    # 認証用トークン。ログイン/登録時に発行し、リクエストの本人確認に使う。
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


# 掲示板メッセージへの応援リアクション（スタンプ）
class Reaction(Base):
    __tablename__ = "reactions"
    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey("messages.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    emoji = Column(String)


# 今日の種: 1日ごとの小さな宣言(チームへのTODO宣言)。
# goal_dateはUTC日付の"YYYY-MM-DD"。芝生・連続記録・サボり点検と同じ
# 日付規約(フロントのtoISOString基準)に統一している。
class DailyGoal(Base):
    __tablename__ = "daily_goals"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    content = Column(String)
    goal_date = Column(String, index=True)
    achieved = Column(Boolean, default=False)
    declared = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utcnow)


# プッシュ通知の購読情報(1ユーザー複数端末を許容)
class PushSubscription(Base):
    __tablename__ = "push_subscriptions"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    endpoint = Column(String, index=True)
    p256dh = Column(String)
    auth = Column(String)
    created_at = Column(DateTime, default=utcnow)


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
    # 既存DBには列が無いため追加する。既存行は NULL のまま。
    if "created_at" not in cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN created_at TIMESTAMP"))
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
        # 送信に失敗した（切断済みの）接続をその場で除去し、
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

# AIメンバーが学習報告に反応するときの応援メッセージ
AI_ENCOURAGEMENTS = [
    "ナイス学習です！その積み重ねが実を結びますよ✨",
    "今日もよく頑張りましたね🍵 しっかり休んでください",
    "コツコツ続けるその姿勢、本当に素晴らしいです🌱",
    "おつかれさまです！一緒にゴールを目指しましょう🔥",
    "今日の一歩が、未来の自分を助けてくれますよ😊",
    "継続は力なり、ですね。応援しています📣",
]

# オンボーディング: チーム参加時にAIが投稿する歓迎メッセージ。
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
    """浮いたAI（group_id=NULLのAIメンバー）を物理削除する掃除処理。

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
            # 今日の種も同様(AIは作らないが、あると削除が失敗し続けるため)
            db.query(DailyGoal).filter(DailyGoal.user_id == ai.id).delete(
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
    """人間が1人もいない「抜け殻グループ」を掃除する。

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


def cleanup_old_daily_goals(db: Session, days: int = 30):
    """古い「今日の種」を掃除する(既定30日より前)。
    種は当日と翌日しか意味を持たないため、古いものは溜める価値がない。
    戻り値: 削除した件数
    """
    cutoff = (datetime.now(JST) - timedelta(days=days)).strftime("%Y-%m-%d")
    deleted = (
        db.query(DailyGoal)
        .filter(DailyGoal.goal_date < cutoff)
        .delete(synchronize_session=False)
    )
    db.commit()
    return deleted


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
        # オンボーディング: 参加直後の掲示板に歓迎メッセージを置く。
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


def send_study_reminders(db: Session):
    """22時の学習リマインダー: 今日まだ報告していない購読者へ通知を送る。
    「今日」の判定は既存規約(UTC日付=芝生・サボり点検と同じ)。
    失効した購読(404/410)はその場で削除する。
    戻り値: (送信成功数, 掃除した失効購読数)
    """
    if not PUSH_ENABLED:
        return 0, 0
    today_start = jst_today_start_utc()
    reported_ids = {
        row[0]
        for row in db.query(Report.user_id)
        .filter(Report.reported_at >= today_start)
        .distinct()
        .all()
    }
    subs = (
        db.query(PushSubscription, User)
        .join(User, PushSubscription.user_id == User.id)
        .filter(User.is_ai == False)
        .all()
    )
    payload = json.dumps(
        {
            "title": "今日はまだ学習していません！",
            "body": "学習をして森を育てましょう",
        }
    )
    sent = 0
    removed = 0
    for sub, target in subs:
        if target.id in reported_ids:
            continue
        try:
            webpush(
                subscription_info={
                    "endpoint": sub.endpoint,
                    "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
                },
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_CLAIMS_EMAIL},
            )
            sent += 1
        except WebPushException as push_error:
            status = getattr(
                getattr(push_error, "response", None), "status_code", None
            )
            if status in (404, 410):
                # 端末側で購読が失効している。データベースから掃除する。
                db.delete(sub)
                db.commit()
                removed += 1
            else:
                print(f"push送信に失敗 (user {target.id}): {push_error}")
        except Exception as push_error:
            print(f"push送信に失敗 (user {target.id}): {push_error}")
    return sent, removed


# --- 毎晩23:59に作動するサボり点検バッチ ---
async def daily_check_task():
    print("🌿 みんスタ サボり監視バッチが正常に起動しました")
    last_reminder_date = None  # 22時リマインダーの送信済み日(重複送信ガード)
    while True:
        try:
            # 22時(JST)の学習リマインダー。60秒間隔のループでも取りこぼさない
            # よう「22時台でその日未送信なら送る」判定にしている(分==0判定は
            # ループのずれで飛ばす恐れがある)。タイムゾーンは明示的にJST。
            jst_now = datetime.now(JST)
            if (
                PUSH_ENABLED
                and jst_now.hour == 22
                and last_reminder_date != jst_now.date()
            ):
                last_reminder_date = jst_now.date()
                reminder_db = SessionLocal()
                try:
                    sent, removed = send_study_reminders(reminder_db)
                    print(
                        f"🔔 学習リマインダーを{sent}件送信"
                        f"(失効購読の掃除{removed}件)"
                    )
                except Exception as remind_error:
                    print(f"リマインダー送信に失敗: {remind_error}")
                finally:
                    reminder_db.close()

            # トリガー判定は従来どおりサーバーのローカル時刻で行う（起動タイミングは不変）。
            now = datetime.now()
            if now.hour == 23 and now.minute == 59:
                db = SessionLocal()
                try:
                    # 改良（バグ修正）: Report.reported_at は UTC で保存されている。
                    # 当日報告の判定は日本時間(JST)の今日0時を基準にする。
                    # 芝生・連続記録・種・通知とすべて同じJST基準で揃えている。
                    today_start = jst_today_start_utc()

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

                    # 抜け殻グループの掃除: 人間が1人もいない（AIだけ/空の）
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

                    # 浮いたAIの掃除: 上の人数調整で外れた分も含めて、
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

                    # 古い「今日の種」の掃除(30日より前)
                    try:
                        old_seeds = cleanup_old_daily_goals(db)
                        if old_seeds:
                            print(f"🧹 古い種を{old_seeds}件掃除しました")
                    except Exception as seed_error:
                        db.rollback()
                        print(f"古い種の掃除に失敗: {seed_error}")
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
# 非推奨の @app.on_event("startup") から lifespan ハンドラへ移行。
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

# allow_origins=["*"] と allow_credentials=True の併用はブラウザ仕様上
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


# PWAアイコン: AndroidのChromeはホーム画面インストールの要件として
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
                    "src": "/icon-192.png?v=2",
                    "sizes": "192x192",
                    "type": "image/png",
                    "purpose": "any",
                },
                {
                    "src": "/icon-512.png?v=2",
                    "sizes": "512x512",
                    "type": "image/png",
                    "purpose": "any",
                },
                {
                    "src": "/icon-512.png?v=2",
                    "sizes": "512x512",
                    "type": "image/png",
                    "purpose": "maskable",
                },
            ],
        }
    )


def _legal_page(title: str, body_html: str) -> str:
    """利用規約・プライバシーポリシーの表示ページ(アプリと同じ世界観の簡易HTML)"""
    return (
        "<!DOCTYPE html><html lang=\"ja\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        "<title>" + title + " | みんスタ</title><style>"
        "body{font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",sans-serif;"
        "max-width:720px;margin:0 auto;padding:24px 18px 60px;color:#5D4037;"
        "background:#FFFDF8;line-height:1.8;}"
        "h1{font-size:22px;text-align:center;color:#2E7D32;margin-bottom:24px;}"
        "h2{font-size:16px;color:#2E7D32;margin-top:28px;border-left:4px solid #A5D6A7;padding-left:10px;}"
        "p{font-size:13px;margin:8px 0;}"
        ".back{display:inline-block;margin-bottom:18px;color:#8D6E63;font-size:13px;text-decoration:none;}"
        "</style></head><body>"
        "<a class=\"back\" href=\"/\">← みんスタにもどる</a>"
        "<h1>" + title + "</h1>" + body_html +
        "</body></html>"
    )


TERMS_BODY_HTML = '<p>本利用規約（以下「本規約」といいます）は、森の主 OH-GY（以下「運営者」といいます）が提供する学習記録共有サービス「みんスタ」（以下「本サービス」といいます）の利用条件を定めるものです。本サービスを利用するすべての方（以下「ユーザー」といいます）は、本規約に同意のうえ、本サービスを利用するものとします。</p>\n<h2>第1条（本サービスの内容）</h2>\n<p>1. 本サービスは、ユーザーが学習の記録を行い、同じ目標を持つ他のユーザーと少人数のチームを組んで、互いの学習状況を共有しながら学習の継続を支援することを目的としたサービスです。</p>\n<p>2. 本サービスには、ユーザーのチームを構成するために、運営者が用意したAIによる仮想のメンバー（以下「AIメンバー」といいます）が含まれる場合があります。詳細は第7条に定めます。</p>\n<p>3. 本サービスは現在、開発中のベータ版（試験提供）です。ユーザーは、本サービスが完成された製品ではなく、不具合・仕様変更・データの消失等が生じうることを理解したうえで利用するものとします。</p>\n<p>4. 運営者は、本サービスの内容を、ユーザーへの事前の通知なく変更・追加・廃止することがあります。</p>\n<h2>第2条（ベータ版であること・データの取り扱い）</h2>\n<p>1. 本サービスはベータ版であるため、運営者は、開発・保守・障害対応・仕様変更等にともない、ユーザーの学習記録その他のデータの全部または一部を、事前の予告なく変更・初期化・削除する場合があります。</p>\n<p>2. ユーザーは、本サービスに記録したデータが永続的に保存されることを保証されないことを、あらかじめ承諾するものとします。重要な記録は、ユーザー自身で別途控えを保管することを推奨します。</p>\n<p>3. データの消失・破損によってユーザーに生じた損害について、運営者は第10条の定めに従い責任を負いません。</p>\n<h2>第3条（利用条件・本規約への同意）</h2>\n<p>1. ユーザーは、本規約に同意した時点で、本サービスを利用できるものとします。</p>\n<p>2. ユーザーが本サービスの新規登録を行う際、または本サービスを実際に利用した時点で、ユーザーは本規約およびプライバシーポリシーの内容を確認し、これらに同意したものとみなされます。</p>\n<p>3. 運営者は、新規登録画面において、本規約およびプライバシーポリシーに同意する旨の明示的な意思表示（チェックボックスへのチェック等）を求めることがあります。この場合、ユーザーは、当該意思表示を行うことで本規約に同意したものとします。</p>\n<p>4. 未成年者が本サービスを利用する場合は、親権者など法定代理人の同意を得たうえで利用するものとします。運営者は、未成年者による利用について、当該同意があったものとして取り扱うことができます。</p>\n<h2>第4条（アカウントの登録・管理）</h2>\n<p>1. ユーザーは、本サービスの利用にあたり、メールアドレス、パスワード、ニックネーム等の必要な情報を、正確かつ最新の内容で登録するものとします。</p>\n<p>2. ユーザーは、登録したパスワードおよびアカウントを、自己の責任において適切に管理するものとし、第三者に利用させ、または貸与・譲渡してはなりません。</p>\n<p>3. パスワードの管理不十分、入力の誤り、第三者の使用等によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<p>4. ニックネームは他のユーザーに表示される場合があります。ユーザーは、本名その他自己を特定できる情報をニックネームに用いないことを推奨されます。</p>\n<h2>第5条（学習記録・投稿内容の取り扱い）</h2>\n<p>1. ユーザーが本サービスに投稿した学習記録、メッセージ、その他の情報（以下「投稿内容」といいます）は、同じチームのユーザー等、本サービスの仕様に応じた範囲で、他のユーザーに表示される場合があります。</p>\n<p>2. 投稿内容に関する責任は、投稿したユーザーが負うものとします。</p>\n<p>3. 投稿内容の著作権は、ユーザーに留保されます。ただし、ユーザーは運営者に対し、本サービスの提供・維持・改善・品質向上・不具合対応のために必要な範囲で、投稿内容を無償かつ非独占的に利用（複製・保存・集計・分析・表示等）する権利を許諾するものとします。</p>\n<p>4. 運営者は、投稿内容を含む本サービスの利用状況を統計的に集計・分析し、個人を特定できない形に加工したうえで、本サービスの紹介、研究、学業上の発表、就職活動その他の目的で、その分析結果を利用・公表することができるものとします。この場合においても、個人を特定できる情報を公表することはありません。</p>\n<p>5. 個人情報の取り扱いについては、別途定めるプライバシーポリシーによります。</p>\n<h2>第6条（禁止事項）</h2>\n<p>ユーザーは、本サービスの利用にあたり、次の行為をしてはなりません。</p>\n<p>(1) 法令または公序良俗に違反する行為</p>\n<p>(2) 他のユーザーまたは第三者の権利・名誉・プライバシーを侵害する行為</p>\n<p>(3) 他のユーザーまたは第三者を誹謗中傷し、不快にさせ、または迷惑をかける行為</p>\n<p>(4) わいせつ、暴力的、差別的その他不適切な内容を、ニックネームや投稿内容に用いる行為</p>\n<p>(5) 虚偽の情報を登録または投稿する行為</p>\n<p>(6) 本サービスの運営を妨害する行為、サーバーやネットワークに過度の負荷をかける行為</p>\n<p>(7) 不正アクセス、その他本サービスのセキュリティを脅かす行為</p>\n<p>(8) 本サービスを、本来の目的（学習の記録・継続支援）以外に利用する行為</p>\n<p>(9) 自動化された手段により本サービスに大量のデータを送信し、または情報を取得する行為</p>\n<p>(10) その他、運営者が不適切と判断する行為</p>\n<h2>第7条（利用の制限・登録の抹消）</h2>\n<p>1. 運営者は、ユーザーが本規約に違反した場合、または違反するおそれがあると判断した場合、事前の通知なく、投稿内容の削除、ニックネームの変更、利用の一時停止、アカウントの削除等の措置を行うことができます。</p>\n<p>2. 前項の措置によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<p>3. 本サービスには、一定期間学習の記録がないユーザーを、自動的にチームから離脱させる仕組みがあります。この仕組みの詳細・基準は、運営者が定め、予告なく変更することがあります。当該離脱によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<h2>第8条（AIメンバーについて）</h2>\n<p>1. 本サービスでは、チームの人数を補うため、運営者が用意したAIメンバーがチームに参加することがあります。AIメンバーは実在の人物ではありません。</p>\n<p>2. チームの構成や人数によっては、ユーザーのチームのメンバーが、AIメンバーのみとなる場合があります。</p>\n<p>3. AIメンバーの発言・反応は自動的に生成されるものであり、その内容の正確性・適切性について運営者は保証せず、責任を負いません。</p>\n<h2>第9条（料金）</h2>\n<p>1. 本サービスは、現在、無料で提供されています。</p>\n<p>2. 運営者は、将来、本サービスの一部の機能を有料で提供することがあります。その場合の料金・支払方法・その他の条件は、別途定め、事前にユーザーに通知します。</p>\n<h2>第10条（サービスの中断・停止・終了）</h2>\n<p>1. 運営者は、次の場合に、ユーザーへの事前の通知なく、本サービスの全部または一部を中断・停止することができます。</p>\n<p>(1) システムの保守・点検・更新を行う場合</p>\n<p>(2) 火災・停電・天災等の不可抗力により、本サービスの提供が困難な場合</p>\n<p>(3) 利用しているサーバー、ネットワーク、外部サービス等に障害が生じた場合</p>\n<p>(4) その他、運営者が必要と判断した場合</p>\n<p>2. 運営者は、本サービスを終了する場合、合理的な方法でユーザーに通知するよう努めます。ただし、ベータ版であることまたは緊急やむを得ない事情がある場合は、事前の通知なく終了することができます。</p>\n<p>3. 本サービスの中断・停止・終了によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<h2>第11条（免責事項）</h2>\n<p>1. 本サービスは、個人により、ベータ版として運営されています。運営者は、本サービスの内容・機能・データの保存等について、完全性・正確性・有用性・継続性・特定目的への適合性を、明示・黙示を問わず保証するものではありません。</p>\n<p>2. 運営者は、本サービスの利用または利用できなかったことによってユーザーに生じた損害（不具合、データの消失・破損、サービスの中断・終了、チームからの自動離脱等を含みます）について、責任を負いません。ただし、運営者の故意または重過失による場合は、この限りではありません。</p>\n<p>3. 前項ただし書きにより運営者が責任を負う場合であっても、その賠償の範囲は、現実に発生した通常かつ直接の損害に限り、本サービスが無償で提供されていることに鑑み、適切な範囲に限られるものとします。</p>\n<p>4. ユーザー間またはユーザーと第三者との間で生じた紛争について、運営者は責任を負いません。ユーザーは、自己の責任と費用において当該紛争を解決するものとします。</p>\n<h2>第12条（本規約の変更）</h2>\n<p>1. 運営者は、必要と判断した場合、本規約を変更することができます。重要な変更を行う場合は、合理的な方法でユーザーに通知または本サービス上に表示します。</p>\n<p>2. 変更後の本規約は、本サービス上に表示された時点から効力を生じるものとします。変更後にユーザーが本サービスを利用した場合、変更後の本規約に同意したものとみなされます。</p>\n<h2>第13条（準拠法・管轄）</h2>\n<p>1. 本規約の解釈・適用は、日本法を準拠法とします。</p>\n<p>2. 本サービスに関して運営者とユーザーとの間で紛争が生じた場合には、運営者の所在地を管轄する裁判所を、第一審の専属的合意管轄裁判所とします。</p>\n<h2>第14条（お問い合わせ・運営者）</h2>\n<p>本サービスの運営者、および本規約に関するお問い合わせ先は、以下のとおりです。</p>\n<p>運営者：森の主 OH-GY</p>\n<p>連絡先：ohgy.dev@gmail.com</p>\n<p>制定日：【※公開日を記入】</p>\n<p>以上</p>'
PRIVACY_BODY_HTML = '<p>森の主 OH-GY（以下「運営者」といいます）は、学習記録共有サービス「みんスタ」（以下「本サービス」といいます）におけるユーザーの個人情報・データの取り扱いについて、以下のとおりプライバシーポリシー（以下「本ポリシー」といいます）を定めます。本サービスはベータ版（試験提供）です。</p>\n<h2>第1条（取得する情報）</h2>\n<p>運営者は、本サービスの提供にあたり、次の情報を取得します。</p>\n<p>(1) ユーザーが登録時・利用時に入力する情報</p>\n<p>・メールアドレス</p>\n<p>・パスワード（後述のとおり、暗号化（ハッシュ化）して保存します）</p>\n<p>・ニックネーム</p>\n<p>・学習の目標、目標日</p>\n<p>・プロフィール画像（ユーザーが設定した場合）</p>\n<p>(2) ユーザーが本サービスの利用にともない生成する情報</p>\n<p>・学習記録（学習内容、学習時間、記録日時など）</p>\n<p>・登録した参考書等の情報</p>\n<p>・その日ごとの目標（宣言）などの情報</p>\n<p>・チーム内で送信したメッセージ、リアクション</p>\n<p>・チームへの所属状況、学習の継続状況など</p>\n<p>(3) サービスの提供に必要な技術的な情報</p>\n<p>・本サービスの動作・通信に必要な範囲の情報</p>\n<p>・プッシュ通知の送信に必要な購読情報（ユーザーが通知を有効にした場合）</p>\n<h2>第2条（利用目的）</h2>\n<p>運営者は、取得した情報を、次の目的のために利用します。</p>\n<p>(1) 本サービスの提供・維持・運営のため</p>\n<p>(2) ユーザーのチーム編成、学習記録の表示、チーム内での共有など、本サービスの機能を提供するため</p>\n<p>(3) ユーザーが有効にした場合に、学習を促すプッシュ通知を送信するため</p>\n<p>(4) 本サービスの不具合対応、安全性の確保、不正利用の防止のため</p>\n<p>(5) 本サービスの改善・品質向上のため</p>\n<p>(6) 本サービスの利用状況を統計的に分析し、個人を特定できない形に加工したうえで、本サービスの紹介・研究・学業上の発表・就職活動等に利用するため</p>\n<p>(7) ユーザーからのお問い合わせに対応するため</p>\n<p>(8) その他、上記に付随する目的のため</p>\n<h2>第3条（統計データ・匿名加工情報の利用）</h2>\n<p>1. 運営者は、取得した情報を統計的に集計・分析し、特定の個人を識別できないように加工した情報（以下「統計データ」といいます）を作成することがあります。</p>\n<p>2. 運営者は、統計データを、本サービスの紹介・改善、研究、学業上の発表、就職活動その他の目的で、利用・公表することができます。統計データには、特定の個人を識別できる情報（メールアドレス、ニックネーム等）は含めません。</p>\n<h2>第4条（パスワードの取り扱い）</h2>\n<p>ユーザーのパスワードは、暗号化（ハッシュ化）した状態で保存します。運営者は、ユーザーの生のパスワードを保持しません。</p>\n<h2>第5条（情報の共有・第三者への提供）</h2>\n<p>1. ユーザーが本サービスに入力・投稿した情報のうち、ニックネーム、学習記録、メッセージ等は、本サービスの仕様に応じて、同じチームのユーザー等、他のユーザーに表示される場合があります。メールアドレスやパスワードが他のユーザーに表示されることはありません。</p>\n<p>2. 運営者は、次の場合を除き、ユーザーの個人情報を第三者に提供しません。</p>\n<p>(1) ユーザーの同意がある場合</p>\n<p>(2) 法令に基づき開示が必要な場合</p>\n<p>(3) 人の生命・身体・財産の保護のために必要であり、本人の同意を得ることが困難な場合</p>\n<h2>第6条（外部サービスの利用）</h2>\n<p>1. 本サービスは、参考書等の書籍情報の検索機能のため、Google LLCが提供する「Google Books API」を利用しています。ユーザーが書籍を検索する際、入力された検索語句が同社に送信され、検索結果として書籍情報を取得します。同社における情報の取り扱いは、同社の定めるプライバシーポリシー（https://policies.google.com/privacy 等）によります。</p>\n<p>2. 本サービスは、その他の機能のため、外部のサービス（API等）を利用することがあります。その際、機能の提供に必要な範囲の情報が外部サービスに送信される場合があります。</p>\n<p>3. 本サービスは、サーバー・データベース等のために、外部のホスティング事業者のサービスを利用しています。ユーザーのデータは、当該事業者のサーバー上に保存されます。</p>\n<p>4. 本サービスのプッシュ通知は、ブラウザおよびOSの提供する配信の仕組みを通じて送信されます。</p>\n<p>5. 本サービスは、第三者によるアクセス解析ツールおよび第三者の広告サービスを使用していません。</p>\n<h2>第7条（情報の管理・保護）</h2>\n<p>運営者は、取得した情報の漏えい・滅失・毀損の防止その他の安全管理のために、必要かつ適切な措置を講じるよう努めます。ただし、本サービスは個人によりベータ版として運営されており、運営者は情報の安全性について完全性を保証するものではありません。</p>\n<h2>第8条（情報の保存期間・削除）</h2>\n<p>1. 運営者は、利用目的の達成に必要な範囲で、ユーザーの情報を保存します。</p>\n<p>2. 本サービスはベータ版であるため、運営者は、開発・保守等にともない、ユーザーの情報の全部または一部を、事前の予告なく削除・初期化する場合があります。</p>\n<p>3. ユーザーが退会（アカウントの削除）を行った場合、運営者は、ユーザーの個人を特定できる情報（メールアドレス、ニックネーム等）を、運営上必要な期間内に削除または個人を特定できない形に加工します。ただし、不具合対応・不正利用の防止・バックアップ等のため、一定期間これらの情報を保持することがあります。</p>\n<p>4. 前項にかかわらず、個人を特定できない形に加工された統計データは、退会後も保持・利用されることがあります。</p>\n<p>5. ユーザーは、自己の情報の開示・訂正・削除等を希望する場合、第9条のお問い合わせ先に連絡することができます。運営者は、本人からの請求であることを確認のうえ、法令に従い、合理的な範囲で対応します。</p>\n<h2>第9条（お問い合わせ先）</h2>\n<p>本ポリシーに関するお問い合わせ、および個人情報の取り扱いに関するご請求は、以下までご連絡ください。</p>\n<p>運営者：森の主 OH-GY</p>\n<p>連絡先：ohgy.dev@gmail.com</p>\n<h2>第10条（本ポリシーの変更）</h2>\n<p>1. 運営者は、必要と判断した場合、本ポリシーを変更することがあります。</p>\n<p>2. 変更後の本ポリシーは、本サービス上に表示された時点から効力を生じるものとします。</p>\n<p>制定日：【※公開日を記入】</p>\n<p>以上</p>'


@app.get("/terms")
def get_terms():
    return HTMLResponse(_legal_page("利用規約", TERMS_BODY_HTML))


@app.get("/privacy")
def get_privacy():
    return HTMLResponse(_legal_page("プライバシーポリシー", PRIVACY_BODY_HTML))


SW_JS = """self.addEventListener('install', function (event) { self.skipWaiting(); });
self.addEventListener('activate', function (event) { event.waitUntil(self.clients.claim()); });
self.addEventListener('fetch', function (event) { });
// push 通知の受信時にシステム通知を表示する
self.addEventListener('push', function (event) {
    var data = {};
    try { data = event.data ? event.data.json() : {}; } catch (e) { }
    var title = data.title || 'みんスタ';
    event.waitUntil(self.registration.showNotification(title, {
        body: data.body || '',
        icon: '/icon-192.png?v=2',
        badge: '/icon-192.png?v=2',
        data: { url: '/' }
    }));
});
self.addEventListener('notificationclick', function (event) {
    event.notification.close();
    event.waitUntil(clients.openWindow('/'));
});
"""


@app.get("/sw.js")
def get_sw():
    return PlainTextResponse(SW_JS, media_type="application/javascript")


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
    # 必須項目の欠落・空文字を検証する(未検証だと KeyError で 500 になる)。
    email = (user_data.get("email") or "").strip()
    password = user_data.get("password") or ""
    name = (user_data.get("name") or "").strip()
    goal = (user_data.get("goal") or "").strip()
    if not email or not password or not name or not goal:
        raise HTTPException(status_code=400, detail="必須項目が入力されていません")
    # メールアドレスの形式チェック(@ とドメイン部の . の有無)。
    if "@" not in email or "." not in email.split("@")[-1]:
        raise HTTPException(status_code=400, detail="メールアドレスの形式が正しくありません")
    # bcrypt は72バイトを超えると例外を投げる。事前に検証して 400 を返す。
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(
            status_code=400,
            detail="パスワードが長すぎます（日本語なら24文字、英数字なら72文字までを目安にしてください）",
        )
    if len(name) > 20:
        raise HTTPException(status_code=400, detail="名前は20文字以内で入力してください")

    existing_user = db.query(User).filter(User.email == email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    new_user = None
    try:
        salt = bcrypt.gensalt()
        hashed_pw = bcrypt.hashpw(password.encode("utf-8"), salt).decode(
            "utf-8"
        )
        new_human_id = get_custom_id(db, is_ai=False)
        new_user = User(
            id=new_human_id,
            email=email,
            hashed_password=hashed_pw,
            name=name,
            goal=goal,
            target_date=user_data.get("target_date"),
            auth_token=issue_token(),
            created_at=utcnow(),
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
    # 認証: 発行したトークンを返す。ブラウザはこれを保存し、以降の通信に使う。
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
    # 認証: ログインのたびに新しいトークンを発行する。
    user.auth_token = issue_token()
    db.commit()
    return {
        "user": {"id": user.id, "name": user.name, "goal": user.goal},
        "token": user.auth_token,
    }


# 認証: ログアウト。サーバー側のトークンを無効化する。
@app.post("/users/logout")
def logout_user(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    current_user.auth_token = None
    db.commit()
    return {"message": "Logged out"}


# 認証: ログイン中ユーザー自身の情報を返す。
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


@app.post("/users/{user_id}/join_group")
async def join_group(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # group_id が NULL のユーザー(キック/卒業で離脱)を再マッチングする。
    require_self(current_user, user_id)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    # 所属済みなら何もしない(二重参加の防止)。
    if user.group_id:
        return {"message": "Already in a group", "group_id": user.group_id}
    # goal が空だとマッチング対象を絞れないため 400 を返す。
    if not (user.goal or "").strip():
        raise HTTPException(status_code=400, detail="先に目標を設定してください")
    # 再参加時は strike_count をリセットする。
    user.strike_count = 0
    db.commit()
    assign_group_logic(db, user)
    if user.group_id:
        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Joined", "group_id": user.group_id}


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


# ===== プッシュ通知(購読の登録/解除) =====
@app.get("/push/public-key")
def get_push_public_key():
    # フロントが購読時に使う公開鍵。未設定なら機能オフを伝える。
    return JSONResponse({"enabled": PUSH_ENABLED, "key": VAPID_PUBLIC_KEY})


@app.post("/push/subscribe")
def push_subscribe(
    sub_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not PUSH_ENABLED:
        raise HTTPException(status_code=503, detail="通知機能は現在準備中です")
    endpoint = (sub_data.get("endpoint") or "").strip()
    keys = sub_data.get("keys") or {}
    p256dh = keys.get("p256dh")
    auth = keys.get("auth")
    if not endpoint or not p256dh or not auth:
        raise HTTPException(status_code=400, detail="購読情報が不正です")
    # 同じ端末(endpoint)の重複登録は上書き(端末でのユーザー切替にも対応)
    existing = (
        db.query(PushSubscription)
        .filter(PushSubscription.endpoint == endpoint)
        .first()
    )
    if existing:
        existing.user_id = current_user.id
        existing.p256dh = p256dh
        existing.auth = auth
    else:
        db.add(
            PushSubscription(
                user_id=current_user.id,
                endpoint=endpoint,
                p256dh=p256dh,
                auth=auth,
            )
        )
    db.commit()
    return {"message": "subscribed"}


@app.post("/push/unsubscribe")
def push_unsubscribe(
    sub_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    endpoint = (sub_data.get("endpoint") or "").strip()
    if endpoint:
        db.query(PushSubscription).filter(
            PushSubscription.endpoint == endpoint,
            PushSubscription.user_id == current_user.id,
        ).delete(synchronize_session=False)
        db.commit()
    return {"message": "unsubscribed"}


# ===== 今日の種(1日ごとの小さな宣言) =====
DAILY_GOAL_MAX_PER_DATE = 3


def jst_today_start_utc():
    """「日本時間での今日の0時」に対応するUTC時刻を返す。
    reported_at等はDB上UTCで保存されているため、「JSTの今日に報告したか」を
    判定するには、JST 0時をUTCに直した時刻(=前日15時UTC)以降かで比較する。
    これで芝生・連続記録・サボり点検・通知の当日判定がすべてJST基準で一致する。"""
    now_jst = datetime.now(JST)
    jst_midnight = datetime(now_jst.year, now_jst.month, now_jst.day, tzinfo=JST)
    return jst_midnight.astimezone(timezone.utc).replace(tzinfo=None)


def seed_dates():
    """今日と明日の日付文字列（日本時間=JST基準）。
    種は「今日やることの宣言」という生活時間に密着した機能なので、
    UTCではなくユーザーの体感に合うJSTで判定する。
    （UTC基準だと、日本の朝9時を過ぎるまで翌日に繰り上がらないため）"""
    now_jst = datetime.now(JST)
    return now_jst.strftime("%Y-%m-%d"), (now_jst + timedelta(days=1)).strftime("%Y-%m-%d")


@app.get("/daily-goals")
async def get_daily_goals(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    today, tomorrow = seed_dates()
    goals = (
        db.query(DailyGoal)
        .filter(
            DailyGoal.user_id == current_user.id,
            DailyGoal.goal_date.in_([today, tomorrow]),
        )
        .order_by(DailyGoal.id)
        .all()
    )
    todays = [g for g in goals if g.goal_date == today]
    # 朝の宣言(案A): その日最初にここへアクセスした時、未宣言の今日の種が
    # あれば掲示板へ宣言を1通だけ流す。宣言は1日1回(後から追加した種は
    # 騒音防止のため宣言に含めない=POST側でdeclared=True扱いにする)。
    undeclared = [g for g in todays if not g.declared]
    already_declared = any(g.declared for g in todays)
    if undeclared and not already_declared and current_user.group_id:
        try:
            nums = ["①", "②", "③"]
            lines = [
                f"{nums[i] if i < 3 else '・'} {g.content}"
                for i, g in enumerate(todays)
            ]
            db.add(
                Message(
                    group_id=current_user.group_id,
                    user_id=current_user.id,
                    content="【今日の種】\n" + "\n".join(lines),
                )
            )
            for g in todays:
                g.declared = True
            db.commit()
            await manager.broadcast_to_group(current_user.group_id, "update")
        except Exception as e:
            db.rollback()
            print(f"seed declaration failed (user {current_user.id}): {e}")
    return {
        "today": [
            {"id": g.id, "content": g.content, "achieved": g.achieved}
            for g in todays
        ],
        "tomorrow": [
            {"id": g.id, "content": g.content, "achieved": g.achieved}
            for g in goals
            if g.goal_date == tomorrow
        ],
    }


@app.post("/daily-goals")
def create_daily_goal(
    goal_data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    content = (goal_data.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="内容を入力してください")
    if len(content) > 50:
        raise HTTPException(status_code=400, detail="種は50文字以内で入力してください")
    today, tomorrow = seed_dates()
    goal_date = tomorrow if goal_data.get("date") == "tomorrow" else today
    existing = (
        db.query(DailyGoal)
        .filter(
            DailyGoal.user_id == current_user.id,
            DailyGoal.goal_date == goal_date,
        )
        .all()
    )
    if len(existing) >= DAILY_GOAL_MAX_PER_DATE:
        raise HTTPException(status_code=400, detail="種は1日3つまでです")
    # その日の宣言が済んだ後に追加された種は、宣言済み扱い(再宣言しない)
    already_declared = any(g.declared for g in existing)
    g = DailyGoal(
        user_id=current_user.id,
        content=content,
        goal_date=goal_date,
        declared=(goal_date == today and already_declared),
    )
    db.add(g)
    db.commit()
    db.refresh(g)
    return {"id": g.id, "content": g.content, "achieved": g.achieved}


@app.patch("/daily-goals/{goal_id}/achieve")
async def achieve_daily_goal(
    goal_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    g = (
        db.query(DailyGoal)
        .filter(DailyGoal.id == goal_id, DailyGoal.user_id == current_user.id)
        .first()
    )
    if not g:
        raise HTTPException(status_code=404, detail="種が見つかりません")
    if g.achieved:
        return {"message": "already achieved"}
    g.achieved = True
    db.commit()
    # 全達成のお祝い: 達成操作で「今日の種が全部達成」に変わった時だけ1通。
    # 未達成は晒さない(達成だけを祝う非対称設計)。
    today, _ = seed_dates()
    if g.goal_date == today and current_user.group_id:
        todays = (
            db.query(DailyGoal)
            .filter(
                DailyGoal.user_id == current_user.id,
                DailyGoal.goal_date == today,
            )
            .all()
        )
        if todays and all(t.achieved for t in todays):
            try:
                db.add(
                    Message(
                        group_id=current_user.group_id,
                        user_id=current_user.id,
                        content=f"【今日の種】{current_user.name}さんの種が今日もぜんぶ芽吹きました",
                    )
                )
                db.commit()
                await manager.broadcast_to_group(current_user.group_id, "update")
            except Exception as e:
                db.rollback()
                print(f"seed celebration failed (user {current_user.id}): {e}")
    return {"message": "achieved"}


@app.delete("/daily-goals/{goal_id}")
def delete_daily_goal(
    goal_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    g = (
        db.query(DailyGoal)
        .filter(DailyGoal.id == goal_id, DailyGoal.user_id == current_user.id)
        .first()
    )
    if not g:
        raise HTTPException(status_code=404, detail="種が見つかりません")
    db.delete(g)
    db.commit()
    return {"message": "deleted"}


@app.get("/groups/{group_id}/members")
def get_members(
    group_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # 認証: 自分が所属するチームの情報のみ閲覧できる。
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
    # 合計の算出を DB 側の集計に委ねる（返却値は従来と同一）。
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


# 表紙画像のアップデートも処理できるように改良
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
    # 認証: 自分の参考書だけを編集できる。
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
    # 認証: 自分の参考書だけを削除できる。
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
    # 認証: 学習記録は必ずログイン中ユーザー本人のものとして登録する。
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

        # グループにいるAIメンバーが、一定の確率で応援メッセージを投稿する。
        # 過疎なチームでも反応が返ってくることで「続けやすい」体験を作る。
        ai_members = (
            db.query(User)
            .filter(User.group_id == user.group_id, User.is_ai == True)
            .all()
        )
        # オンボーディング: 初回報告だけは反応を運任せにしない。
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
                # はフロントの応援スタンプ定番セット（STICKERS）にある絵文字
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
    # 認証: 自分の学習記録だけを削除できる。
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
    # 認証: 自分が所属するチームの掲示板のみ閲覧できる。
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

    # 各メッセージへの応援リアクションを 1 クエリでまとめて取得する。
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
    # 認証: 自分が所属するチームにのみ、本人として投稿できる。
    if current_user.group_id != group_id:
        raise HTTPException(
            status_code=403, detail="このチームには投稿できません"
        )
    # 空メッセージと過大な文字数を検証する。
    content = (msg_data.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="メッセージが空です")
    if len(content) > 1000:
        raise HTTPException(
            status_code=400, detail="メッセージは1000文字以内で入力してください"
        )
    msg = Message(
        group_id=group_id, user_id=current_user.id, content=content
    )
    db.add(msg)
    db.commit()
    await manager.broadcast_to_group(group_id, "update")
    return {"message": "Message posted"}


# メッセージへの応援リアクション（スタンプ）をトグルする。
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
    # 認証: 自分が所属するチームのメッセージにのみ反応できる。
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


# セキュリティ改良: 書籍検索のサーバー側プロキシ。
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