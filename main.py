from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    Header,
    Request,
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
import re
import secrets
import hashlib
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
    # どの塾のグループか。None なら塾に属さない一般のグループ。
    juku_id = Column(Integer, ForeignKey("jukus.id"), nullable=True, index=True)


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
    # 所属する塾。None なら個人利用（これまで通りの動作）。
    juku_id = Column(Integer, ForeignKey("jukus.id"), nullable=True, index=True)
    # 登録のきっかけになった経路（広告・SNSなど）。URLの ?src= の短い印だけを保存する。
    # 広告の効果を測るためのもので、個人を特定する情報は含まない。
    signup_source = Column(String, nullable=True, index=True)
    is_developer = Column(Boolean, default=False)
    is_advisor = Column(Boolean, default=False)
    is_tester = Column(Boolean, default=False)
    # 登録日時(UTC)。既存ユーザーは NULL。コホート分析に使用。
    created_at = Column(DateTime, nullable=True, default=utcnow)
    # 認証用トークン。ログイン/登録時に発行し、リクエストの本人確認に使う。
    # 旧方式の名残。現在は auth_tokens テーブルで管理しており、この列は使用しない。
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
    # 当日判定・芝生・連続記録・サボり点検で範囲検索するため索引を張る
    reported_at = Column(DateTime, default=utcnow, index=True)


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


class Juku(Base):
    """塾。塾向け展開の土台。

    生徒は塾コードを入力すると、その塾に所属する。
    所属すると、チームは「同じ塾の中の、同じ目標の生徒」から組まれる。
    塾に所属していない利用者はこれまで通り、誰とでも組まれる。
    """
    __tablename__ = "jukus"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    code = Column(String, unique=True, index=True)  # 生徒に配る合言葉
    created_at = Column(DateTime, default=utcnow)


class AuthToken(Base):
    """ログインセッション。1ユーザーが複数端末で同時にログインできる。

    トークンそのものは保存せず、SHA-256のハッシュだけを持つ。
    こうしておくと、万一データベースの中身が漏れても、
    そこからログインできるトークンを復元することはできない。
    （パスワードをハッシュで持つのと同じ考え方）
    """
    __tablename__ = "auth_tokens"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    token_hash = Column(String, unique=True, index=True)
    expires_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utcnow)


class PomodoroAlarm(Base):
    """ポモドーロ完了通知の予約（1ユーザー1件）。
    アプリがバックグラウンドでOSに停止されると、25分経過の鐘を鳴らせない。
    そこで開始時にサーバへ「25分後に知らせて」と予約し、バッチが期限を
    迎えた予約へプッシュ通知を送る。アプリが生きていれば完了時に予約を
    取り消すため、通知が届くのは端末側で鐘が鳴らせなかった場合だけ。"""
    __tablename__ = "pomodoro_alarms"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    notify_at = Column(DateTime, index=True)
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

    # 塾向け展開で追加した列。既存の利用者・グループは NULL（＝塾に属さない）になり、
    # これまでと同じ動作を続ける。これが無いと、利用者を読むたびに
    # 「列が存在しない」エラーになり、アプリ全体が動かなくなる。
    # 登録経路の列（広告の効果測定用）
    if "signup_source" not in cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN signup_source VARCHAR"))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_users_signup_source ON users (signup_source)"))
            conn.commit()

    with engine.connect() as conn:
        for table in ("users", "groups"):
            if not inspector.has_table(table):
                continue
            tcols = [c["name"] for c in inspector.get_columns(table)]
            if "juku_id" not in tcols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN juku_id INTEGER"))
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS ix_{table}_juku_id ON {table} (juku_id)"))
        # 当日判定やサボり点検で範囲検索する列の索引（create_allは既存表に索引を足さない）
        if inspector.has_table("reports"):
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_reports_reported_at ON reports (reported_at)"))
        conn.commit()


ensure_schema()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- 認証 ---
def clean_source(value):
    """登録経路の印を検証する。英数字と - _ . だけ、64文字まで。合わなければ保存しない。
    （URLに付けた印なので誰でも書き換えられる。変な文字列をDBに入れないため）"""
    if not isinstance(value, str):
        return None
    value = value.strip()
    if 1 <= len(value) <= 64 and re.fullmatch(r"[A-Za-z0-9_.\-]+", value):
        return value
    return None


# ===== ログイン試行の制限（総当たり対策） =====
# 外部ライブラリを増やさず、メモリ上で数えるだけの簡易版。
# サーバーを再起動すると記録は消えるが、短時間に何千回と試す攻撃は防げる。
LOGIN_MAX_FAILURES = 5          # 何回失敗したらロックするか
LOGIN_WINDOW_SECONDS = 300      # 失敗を数える時間の幅（5分）
LOGIN_LOCK_SECONDS = 900        # ロックする時間（15分）

# キー -> 失敗した時刻のリスト
_login_failures: Dict[str, List[datetime]] = {}


def _login_keys(request: Request, email: str) -> List[str]:
    """IPアドレスとメールアドレスの両方で数える。
    IPだけだと共用回線で巻き添えが出るし、メールだけだと
    相手を変えながら総当たりできてしまうため、両方を見る。
    """
    ip = request.client.host if request and request.client else "unknown"
    return [f"ip:{ip}", f"mail:{email.lower()}"]


def check_login_rate(request: Request, email: str):
    """失敗が続いているならロックする。ログイン処理の最初に呼ぶ。"""
    now = utcnow()
    for key in _login_keys(request, email):
        recent = [
            t for t in _login_failures.get(key, [])
            if (now - t).total_seconds() < LOGIN_LOCK_SECONDS
        ]
        _login_failures[key] = recent
        in_window = [
            t for t in recent if (now - t).total_seconds() < LOGIN_WINDOW_SECONDS
        ]
        if len(in_window) >= LOGIN_MAX_FAILURES:
            raise HTTPException(
                status_code=429,
                detail="ログインの試行が続いたため、しばらく受け付けられません。時間をおいて再度お試しください",
            )


def record_login_failure(request: Request, email: str):
    """失敗を記録する。"""
    now = utcnow()
    for key in _login_keys(request, email):
        _login_failures.setdefault(key, []).append(now)


def clear_login_failures(request: Request, email: str):
    """成功したら記録を消す。"""
    for key in _login_keys(request, email):
        _login_failures.pop(key, None)


# ログイン/登録時にセッションを作り、トークンを発行する（保存はハッシュのみ）。
# 以降のリクエストは「Authorization: Bearer <token>」ヘッダーでトークンを送り、
# サーバーはトークンからユーザーを特定する（opaque token 方式）。
# これにより、リクエストの user_id を詐称して他人のデータを操作することを防ぐ。


# ログインを維持する期間。これを過ぎたトークンは無効になる。
TOKEN_LIFETIME_DAYS = 60


def issue_token() -> str:
    """推測できないランダムな文字列を作る（これが利用者に渡る本体）。"""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    """トークンをSHA-256で要約する。保存と照合はこの値だけで行う。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_session(db: Session, user: User) -> str:
    """新しいセッションを作り、利用者に渡すトークンを返す。

    端末ごとに1行増えるだけなので、スマホとPCで同時にログインできる。
    ついでに、その利用者の期限切れセッションを掃除しておく。
    """
    db.query(AuthToken).filter(
        AuthToken.user_id == user.id, AuthToken.expires_at <= utcnow()
    ).delete(synchronize_session=False)
    token = issue_token()
    db.add(
        AuthToken(
            user_id=user.id,
            token_hash=hash_token(token),
            expires_at=utcnow() + timedelta(days=TOKEN_LIFETIME_DAYS),
        )
    )
    db.commit()
    return token


def user_from_token(db: Session, token: str) -> Optional[User]:
    """トークンから利用者を特定する。期限切れ・不正なら None。"""
    if not token:
        return None
    row = (
        db.query(AuthToken)
        .filter(AuthToken.token_hash == hash_token(token))
        .first()
    )
    if not row:
        # 旧方式（users.auth_token に平文で保存していた頃）のトークンなら、
        # 新方式のセッションに移し替えてから通す。移し替えたら平文は消す。
        # これにより、更新後も利用者がログアウトされずに済む。
        legacy = db.query(User).filter(User.auth_token == token).first()
        if not legacy:
            return None
        db.add(AuthToken(
            user_id=legacy.id,
            token_hash=hash_token(token),
            expires_at=utcnow() + timedelta(days=TOKEN_LIFETIME_DAYS),
        ))
        legacy.auth_token = None
        db.commit()
        return legacy
    if row.expires_at and row.expires_at <= utcnow():
        db.delete(row)
        db.commit()
        return None
    return db.query(User).filter(User.id == row.user_id).first()


def get_current_user(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
) -> User:
    """Authorization ヘッダーのトークンから、ログイン中のユーザーを特定する。"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="ログインが必要です")
    token = authorization[len("Bearer "):]
    user = user_from_token(db, token)
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
            # AIメンバーもグループと同じ塾に属させる（塾の集計と整合させるため）
            grp = db.query(Group).filter(Group.id == group_id).first()
            new_ai = User(
                id=new_ai_id,
                name=random.choice(available_names),
                goal=goal,
                group_id=group_id,
                juku_id=grp.juku_id if grp else None,
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
    """利用者をチームに割り当てる。

    塾に所属している場合は同じ塾の中だけで、
    所属していない場合は塾に属さないグループの中から探す。
    どちらも「同じ目標」であることが前提。
    """
    try:
        potential_groups = (
            db.query(Group)
            .filter(Group.goal == user.goal, Group.juku_id == user.juku_id)
            .all()
        )
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
            target_group = Group(goal=user.goal, juku_id=user.juku_id)
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
    return sent, removed


def send_due_pomodoro_alarms(db: Session):
    """期限を迎えたポモドーロ完了通知を送り、予約を削除する。
    バッチのループ(約60秒間隔)から毎周呼ばれるため、通知は期限から
    最大1分ほど遅れて届くことがある。アプリが前面にあれば端末側の鐘が
    正確に鳴り、予約はその時点で取り消されるので二重にはならない。
    失効した購読(404/410)はリマインダーと同様にその場で掃除する。
    戻り値: (送信成功数, 掃除した失効購読数)
    """
    now = utcnow()
    due = db.query(PomodoroAlarm).filter(PomodoroAlarm.notify_at <= now).all()
    if not due:
        return 0, 0

    payload = json.dumps(
        {
            "title": "25分の集中が終わりました",
            "body": "おつかれさまです。アプリを開くと記録が反映されます 🌱",
        }
    )
    sent = 0
    removed = 0
    for alarm in due:
        if PUSH_ENABLED:
            subs = (
                db.query(PushSubscription)
                .filter(PushSubscription.user_id == alarm.user_id)
                .all()
            )
            for sub in subs:
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
                        db.delete(sub)
                        removed += 1
                    else:
                        print(f"ポモ通知に失敗 (user {alarm.user_id}): {push_error}")
                except Exception as push_error:
                    print(f"ポモ通知に失敗 (user {alarm.user_id}): {push_error}")
        # 送信可否に関わらず予約は消化する（push無効環境でも溜めない）
        db.delete(alarm)
    db.commit()
    return sent, removed


# --- 毎晩23:59に作動するサボり点検バッチ ---
async def daily_check_task():
    print("🌿 みんスタ サボり監視バッチが正常に起動しました")
    last_reminder_date = None  # 22時リマインダーの送信済み日(重複送信ガード)
    while True:
        try:
            # ポモドーロ完了通知: 期限が来た予約を毎周(約60秒ごと)点検して送る。
            alarm_db = SessionLocal()
            try:
                p_sent, p_removed = send_due_pomodoro_alarms(alarm_db)
                if p_sent or p_removed:
                    print(
                        f"🔔 ポモドーロ完了通知を{p_sent}件送信"
                        f"(失効購読の掃除{p_removed}件)"
                    )
            except Exception as alarm_error:
                print(f"ポモドーロ通知の点検に失敗: {alarm_error}")
            finally:
                alarm_db.close()

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

            # トリガー判定も日本時間で行う。判定窓が jst_today_start_utc() (JST基準)
            # なのに発火だけサーバーのローカル時刻だと、サーバーがUTCの場合に
            # JST朝9時前後で点検してしまい、「3日連続」より1日早くキックされる。
            now = datetime.now(JST)
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
# CORS: どのサイトからこのAPIを呼べるかの設定。
# 本番と開発用だけを許可する（環境変数 EXTRA_ORIGINS でカンマ区切りで追加できる）。
ALLOWED_ORIGINS = [
    "https://minsta-2h19.onrender.com",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
] + [o.strip() for o in os.environ.get("EXTRA_ORIGINS", "").split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
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


# アプリ内ヘッダーのロゴ。明るい地用と暗い地用の2つを持つ
# （元の絵は茶色が主体で、黒い地では沈んで読みにくくなるため）。
LOGO_WEBP_B64 = "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"
LOGO_DARK_WEBP_B64 = "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"


@app.get("/logo.webp")
def get_logo():
    return Response(
        content=base64.b64decode(LOGO_WEBP_B64),
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=604800"},
    )


@app.get("/logo-dark.webp")
def get_logo_dark():
    return Response(
        content=base64.b64decode(LOGO_DARK_WEBP_B64),
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=604800"},
    )


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


TERMS_BODY_HTML = '<p>本利用規約（以下「本規約」といいます）は、森の主 OH-GY（以下「運営者」といいます）が提供する学習記録共有サービス「みんスタ」（以下「本サービス」といいます）の利用条件を定めるものです。本サービスを利用するすべての方（以下「ユーザー」といいます）は、本規約に同意のうえ、本サービスを利用するものとします。</p>\n<h2>第1条（本サービスの内容）</h2>\n<p>1. 本サービスは、ユーザーが学習の記録を行い、同じ目標を持つ他のユーザーと少人数のチームを組んで、互いの学習状況を共有しながら学習の継続を支援することを目的としたサービスです。</p>\n<p>2. 本サービスには、ユーザーのチームを構成するために、運営者が用意したAIによる仮想のメンバー（以下「AIメンバー」といいます）が含まれる場合があります。詳細は第8条に定めます。</p>\n<p>3. 本サービスは現在、開発中のベータ版（試験提供）です。ユーザーは、本サービスが完成された製品ではなく、不具合・仕様変更・データの消失等が生じうることを理解したうえで利用するものとします。</p>\n<p>4. 運営者は、本サービスの内容を、ユーザーへの事前の通知なく変更・追加・廃止することがあります。</p>\n<p>5. ユーザーは、塾・学校・サークル等（以下「コミュニティ」といいます）が発行するコードを入力することで、そのコミュニティに所属できます。所属したユーザーのチームは、同じコミュニティに所属するユーザーの中から編成されます。ユーザーは、いつでもコミュニティへの所属を解除できます。</p>\n<h2>第2条（ベータ版であること・データの取り扱い）</h2>\n<p>1. 本サービスはベータ版であるため、運営者は、開発・保守・障害対応・仕様変更等にともない、ユーザーの学習記録その他のデータの全部または一部を、事前の予告なく変更・初期化・削除する場合があります。</p>\n<p>2. ユーザーは、本サービスに記録したデータが永続的に保存されることを保証されないことを、あらかじめ承諾するものとします。重要な記録は、ユーザー自身で別途控えを保管することを推奨します。</p>\n<p>3. データの消失・破損によってユーザーに生じた損害について、運営者は第11条の定めに従い責任を負いません。</p>\n<h2>第3条（利用条件・本規約への同意）</h2>\n<p>1. ユーザーは、本規約に同意した時点で、本サービスを利用できるものとします。</p>\n<p>2. ユーザーが本サービスの新規登録を行う際、または本サービスを実際に利用した時点で、ユーザーは本規約およびプライバシーポリシーの内容を確認し、これらに同意したものとみなされます。</p>\n<p>3. 運営者は、新規登録画面において、本規約およびプライバシーポリシーに同意する旨の明示的な意思表示（チェックボックスへのチェック等）を求めることがあります。この場合、ユーザーは、当該意思表示を行うことで本規約に同意したものとします。</p>\n<p>4. 未成年者が本サービスを利用する場合は、親権者など法定代理人の同意を得たうえで利用するものとします。運営者は、未成年者による利用について、当該同意があったものとして取り扱うことができます。</p>\n<h2>第4条（アカウントの登録・管理・退会）</h2>\n<p>1. ユーザーは、本サービスの利用にあたり、メールアドレス、パスワード、ニックネーム等の必要な情報を、正確かつ最新の内容で登録するものとします。パスワードは8文字以上とします。</p>\n<p>2. ユーザーは、登録したパスワードおよびアカウントを、自己の責任において適切に管理するものとし、第三者に利用させ、または貸与・譲渡してはなりません。</p>\n<p>3. パスワードの管理不十分、入力の誤り、第三者の使用等によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<p>4. ニックネームは他のユーザーに表示される場合があります。ユーザーは、本名その他自己を特定できる情報をニックネームに用いないことを推奨されます。</p>\n<p>5. ログインの状態は、ログインした時点から60日が経過すると解除されます。複数の端末で同時にログインすることができ、ログアウトはその端末についてのみ行われます。</p>\n<p>6. 運営者は、不正なログインを防ぐため、一定時間内にログインの失敗が続いた場合、一時的にログインを受け付けないことがあります。</p>\n<p>7. ユーザーは、設定画面から、いつでも退会（アカウントの削除）することができます。退会すると、ユーザーの学習記録、参考書、投稿その他の情報は削除され、元に戻すことはできません。</p>\n<h2>第5条（学習記録・投稿内容の取り扱い）</h2>\n<p>1. ユーザーが本サービスに投稿した学習記録、メッセージ、その他の情報（以下「投稿内容」といいます）は、同じチームのユーザー等、本サービスの仕様に応じた範囲で、他のユーザーに表示される場合があります。</p>\n<p>2. 投稿内容に関する責任は、投稿したユーザーが負うものとします。</p>\n<p>3. 投稿内容の著作権は、ユーザーに留保されます。ただし、ユーザーは運営者に対し、本サービスの提供・維持・改善・品質向上・不具合対応のために必要な範囲で、投稿内容を無償かつ非独占的に利用（複製・保存・集計・分析・表示等）する権利を許諾するものとします。</p>\n<p>4. 運営者は、投稿内容を含む本サービスの利用状況を統計的に集計・分析し、個人を特定できない形に加工したうえで、本サービスの紹介、研究、学業上の発表、就職活動その他の目的で、その分析結果を利用・公表することができるものとします。この場合においても、個人を特定できる情報を公表することはありません。</p>\n<p>5. 個人情報の取り扱いについては、別途定めるプライバシーポリシーによります。</p>\n<p>6. ユーザーがコミュニティに所属した場合でも、運営者は、ユーザーの情報をコミュニティの運営者（塾の先生等）に提供しません。将来、コミュニティの運営者がユーザーの学習状況を閲覧できる機能を提供する場合は、共有する項目を示したうえで、ユーザー（16歳未満の場合はその法定代理人）の同意を得てから開始します。</p>\n<h2>第6条（禁止事項）</h2>\n<p>ユーザーは、本サービスの利用にあたり、次の行為をしてはなりません。</p>\n<p>(1) 法令または公序良俗に違反する行為</p>\n<p>(2) 他のユーザーまたは第三者の権利・名誉・プライバシーを侵害する行為</p>\n<p>(3) 他のユーザーまたは第三者を誹謗中傷し、不快にさせ、または迷惑をかける行為</p>\n<p>(4) わいせつ、暴力的、差別的その他不適切な内容を、ニックネームや投稿内容に用いる行為</p>\n<p>(5) 虚偽の情報を登録または投稿する行為</p>\n<p>(6) 本サービスの運営を妨害する行為、サーバーやネットワークに過度の負荷をかける行為</p>\n<p>(7) 不正アクセス、その他本サービスのセキュリティを脅かす行為</p>\n<p>(8) 本サービスを、本来の目的（学習の記録・継続支援）以外に利用する行為</p>\n<p>(9) 自動化された手段により本サービスに大量のデータを送信し、または情報を取得する行為</p>\n<p>(10) その他、運営者が不適切と判断する行為</p>\n<h2>第7条（利用の制限・チームからの離脱）</h2>\n<p>1. 運営者は、ユーザーが本規約に違反した場合、または違反するおそれがあると判断した場合、事前の通知なく、投稿内容の削除、ニックネームの変更、利用の一時停止、アカウントの削除等の措置を行うことができます。</p>\n<p>2. 前項の措置によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<p>3. 本サービスでは、3日間連続で学習の記録がないユーザーは、チームの席を空けるため、自動的にチームから離脱します。離脱しても学習記録は消えず、ユーザーはいつでもチームに再参加できます。運営者は、この基準を変更する場合、事前に本サービス上で告知します。</p>\n<h2>第8条（AIメンバーについて）</h2>\n<p>1. 本サービスでは、チームの人数を補うため、運営者が用意したAIメンバーがチームに参加することがあります。AIメンバーは実在の人物ではありません。</p>\n<p>2. チームの構成や人数によっては、ユーザー以外のチームのメンバーが、AIメンバーのみとなる場合があります。</p>\n<p>3. AIメンバーの発言・反応は自動的に生成されるものであり、その内容の正確性・適切性について運営者は保証せず、責任を負いません。</p>\n<h2>第9条（料金）</h2>\n<p>1. 本サービスは、現在、無料で提供されています。</p>\n<p>2. 運営者は、将来、本サービスの一部の機能を有料で提供することがあります。その場合の料金・支払方法・その他の条件は、別途定め、事前にユーザーに通知します。</p>\n<h2>第10条（サービスの中断・停止・終了）</h2>\n<p>1. 運営者は、次の場合に、ユーザーへの事前の通知なく、本サービスの全部または一部を中断・停止することができます。</p>\n<p>(1) システムの保守・点検・更新を行う場合</p>\n<p>(2) 火災・停電・天災等の不可抗力により、本サービスの提供が困難な場合</p>\n<p>(3) 利用しているサーバー、ネットワーク、外部サービス等に障害が生じた場合</p>\n<p>(4) その他、運営者が必要と判断した場合</p>\n<p>2. 運営者は、本サービスを終了する場合、合理的な方法でユーザーに通知するよう努めます。ただし、ベータ版であることまたは緊急やむを得ない事情がある場合は、事前の通知なく終了することができます。</p>\n<p>3. 本サービスの中断・停止・終了によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<h2>第11条（免責事項）</h2>\n<p>1. 本サービスは、個人により、ベータ版として運営されています。運営者は、本サービスの内容・機能・データの保存等について、完全性・正確性・有用性・継続性・特定目的への適合性を、明示・黙示を問わず保証するものではありません。</p>\n<p>2. 運営者は、本サービスの利用または利用できなかったことによってユーザーに生じた損害（不具合、データの消失・破損、サービスの中断・終了、チームからの自動離脱等を含みます）について、責任を負いません。ただし、運営者の故意または重過失による場合は、この限りではありません。</p>\n<p>3. 前項ただし書きにより運営者が責任を負う場合であっても、その賠償の範囲は、現実に発生した通常かつ直接の損害に限り、本サービスが無償で提供されていることに鑑み、適切な範囲に限られるものとします。</p>\n<p>4. ユーザー間またはユーザーと第三者との間で生じた紛争について、運営者は責任を負いません。ユーザーは、自己の責任と費用において当該紛争を解決するものとします。</p>\n<h2>第12条（本規約の変更）</h2>\n<p>1. 運営者は、必要と判断した場合、本規約を変更することができます。重要な変更を行う場合は、合理的な方法でユーザーに通知または本サービス上に表示します。</p>\n<p>2. 変更後の本規約は、本サービス上に表示された時点から効力を生じるものとします。変更後にユーザーが本サービスを利用した場合、変更後の本規約に同意したものとみなされます。</p>\n<h2>第13条（準拠法・管轄）</h2>\n<p>1. 本規約の解釈・適用は、日本法を準拠法とします。</p>\n<p>2. 本サービスに関して運営者とユーザーとの間で紛争が生じた場合には、運営者の所在地を管轄する裁判所を、第一審の専属的合意管轄裁判所とします。</p>\n<h2>第14条（お問い合わせ・運営者）</h2>\n<p>本サービスの運営者、および本規約に関するお問い合わせ先は、以下のとおりです。</p>\n<p>運営者：森の主 OH-GY</p>\n<p>連絡先：ohgy.dev@gmail.com</p>\n<p>制定日：2026年6月27日</p>\n<p>改定日：2026年9月21日</p>\n<p>以上</p>'
PRIVACY_BODY_HTML = '<p>森の主 OH-GY（以下「運営者」といいます）は、学習記録共有サービス「みんスタ」（以下「本サービス」といいます）におけるユーザーの個人情報・データの取り扱いについて、以下のとおりプライバシーポリシー（以下「本ポリシー」といいます）を定めます。本サービスはベータ版（試験提供）です。</p>\n<h2>第1条（取得する情報）</h2>\n<p>運営者は、本サービスの提供にあたり、次の情報を取得します。</p>\n<p>(1) ユーザーが登録時・利用時に入力する情報</p>\n<p>・メールアドレス</p>\n<p>・パスワード（第4条のとおり、元に戻せない形に変換（ハッシュ化）して保存します）</p>\n<p>・ニックネーム</p>\n<p>・学習の目標、目標日</p>\n<p>・プロフィール画像（ユーザーが設定した場合）</p>\n<p>・所属するコミュニティ（ユーザーがコミュニティのコードを入力した場合）</p>\n<p>(2) ユーザーが本サービスの利用にともない生成する情報</p>\n<p>・学習記録（学習内容、学習時間、記録日時など）</p>\n<p>・登録した参考書等の情報</p>\n<p>・その日ごとの目標（宣言）などの情報</p>\n<p>・チーム内で送信したメッセージ、リアクション</p>\n<p>・チームへの所属状況、学習の継続状況など</p>\n<p>(3) サービスの提供に必要な技術的な情報</p>\n<p>・ログインの状態を保つための情報（ハッシュ化したトークンと有効期限）</p>\n<p>・ログインの試行を制限するための情報（IPアドレスとメールアドレス。ログインに失敗した場合に限り、サーバーのメモリ上に最大15分間保持し、データベースには保存しません）</p>\n<p>・プッシュ通知の送信に必要な購読情報（ユーザーが通知を有効にした場合）</p>\n<p>・25分集中の終了を知らせる通知の予約（ユーザーが25分集中を開始した場合）</p>\n<p>・登録のきっかけになった経路（広告やSNSなど。リンクに付けた短い印だけで、個人を特定する情報は含みません）</p>\n<p>(4) ユーザーの端末内に保存する情報</p>\n<p>本サービスは、次の情報をユーザーの端末（ブラウザ）内に保存します。このうち、ログインの状態を示す情報以外は、運営者に送信されません。</p>\n<p>・ログインの状態を示す情報\u3000・表示の設定（ダークテーマ等）\u3000・計測中のタイマーの状態\u3000・案内を表示したかどうかの記録</p>\n<h2>第2条（利用目的）</h2>\n<p>運営者は、取得した情報を、次の目的のために利用します。</p>\n<p>(1) 本サービスの提供・維持・運営のため</p>\n<p>(2) ユーザーのチーム編成、学習記録の表示、チーム内での共有など、本サービスの機能を提供するため</p>\n<p>(3) ユーザーが有効にした場合に、学習を促すプッシュ通知や、25分集中の終了の通知を送信するため</p>\n<p>(4) 本サービスの不具合対応、安全性の確保、不正なログインや不正利用の防止のため</p>\n<p>(5) 本サービスの改善・品質向上のため（広告やSNSなど、どの経路からの登録が多いかの把握を含みます）</p>\n<p>(6) 本サービスの利用状況を統計的に分析し、個人を特定できない形に加工したうえで、本サービスの紹介・研究・学業上の発表・就職活動等に利用するため</p>\n<p>(7) ユーザーからのお問い合わせに対応するため</p>\n<p>(8) その他、上記に付随する目的のため</p>\n<h2>第3条（統計データ・匿名加工情報の利用）</h2>\n<p>1. 運営者は、取得した情報を統計的に集計・分析し、特定の個人を識別できないように加工した情報（以下「統計データ」といいます）を作成することがあります。</p>\n<p>2. 運営者は、統計データを、本サービスの紹介・改善、研究、学業上の発表、就職活動その他の目的で、利用・公表することができます。統計データには、特定の個人を識別できる情報（メールアドレス、ニックネーム等）は含めません。</p>\n<h2>第4条（パスワード・ログイン情報の取り扱い）</h2>\n<p>1. ユーザーのパスワードは、元に戻せない形に変換（ハッシュ化）した状態で保存します。運営者は、ユーザーの生のパスワードを保持しません。</p>\n<p>2. ログインの状態を保つためのトークンも、ハッシュ化した状態で保存し、発行から60日で失効させます。</p>\n<h2>第5条（情報の共有・第三者への提供）</h2>\n<p>1. ユーザーが本サービスに入力・投稿した情報のうち、ニックネーム、学習記録、メッセージ等は、本サービスの仕様に応じて、同じチームのユーザー等、他のユーザーに表示される場合があります。メールアドレスやパスワードが他のユーザーに表示されることはありません。</p>\n<p>2. 運営者は、次の場合を除き、ユーザーの個人情報を第三者に提供しません。</p>\n<p>(1) ユーザーの同意がある場合</p>\n<p>(2) 法令に基づき開示が必要な場合</p>\n<p>(3) 人の生命・身体・財産の保護のために必要であり、本人の同意を得ることが困難な場合</p>\n<p>3. ユーザーがコミュニティに所属した場合でも、運営者は、ユーザーの情報をコミュニティの運営者（塾の先生等）に提供しません。所属によって変わるのは、チームを編成する範囲だけです。将来、コミュニティの運営者がユーザーの学習状況を閲覧できる機能を提供する場合は、共有する項目と共有しない項目を示したうえで、ユーザー（16歳未満の場合はその法定代理人）の同意を得てから開始します。</p>\n<h2>第6条（外部サービスの利用）</h2>\n<p>1. 本サービスは、参考書等の書籍情報の検索機能のため、Google LLCが提供する「Google Books API」を利用しています。ユーザーが書籍を検索する際、入力された検索語句が同社に送信され、検索結果として書籍情報を取得します。同社における情報の取り扱いは、同社の定めるプライバシーポリシー（https://policies.google.com/privacy 等）によります。</p>\n<p>2. 本サービスの画面は、表示に使う文字（Google LLCの「Google Fonts」）、グラフの描画に使うプログラム（jsDelivrが配信する「Chart.js」）、環境音（mixkit および Wikimedia Commons が配信する音声ファイル）を、ユーザーの端末から直接読み込みます。その際、ユーザーのIPアドレスやブラウザの情報が、それぞれの提供元に送信されます。</p>\n<p>3. 本サービスのサーバーおよびデータベースは、米国の事業者であるRender（https://render.com）が提供するサービスを利用しています。ユーザーのデータは、同社のサーバー上に保存されます。</p>\n<p>4. 本サービスのプッシュ通知は、ブラウザおよびOSの提供する配信の仕組みを通じて送信されます。</p>\n<p>5. 本サービスは、第三者によるアクセス解析ツールおよび第三者の広告サービスを使用していません。</p>\n<h2>第7条（情報の管理・保護）</h2>\n<p>運営者は、取得した情報の漏えい・滅失・毀損の防止その他の安全管理のために、必要かつ適切な措置を講じるよう努めます。ただし、本サービスは個人によりベータ版として運営されており、運営者は情報の安全性について完全性を保証するものではありません。</p>\n<h2>第8条（情報の保存期間・削除）</h2>\n<p>1. 運営者は、利用目的の達成に必要な範囲で、ユーザーの情報を保存します。主な情報の保存期間は次のとおりです。</p>\n<p>・ログインの状態：発行から60日で失効し、削除します</p>\n<p>・ログインの試行の記録：最大15分間（サーバーのメモリ上のみ）</p>\n<p>・その日ごとの目標（宣言）：30日を過ぎたものを自動で削除します</p>\n<p>・25分集中の終了通知の予約：通知した時点、または取り消した時点で削除します</p>\n<p>2. 本サービスはベータ版であるため、運営者は、開発・保守等にともない、ユーザーの情報の全部または一部を、事前の予告なく削除・初期化する場合があります。</p>\n<p>3. ユーザーは、設定画面から、いつでも退会（アカウントの削除）することができます。退会すると、運営者は、アカウント、学習記録、参考書、その日ごとの目標、メッセージ、リアクション（ユーザーのメッセージに他のユーザーが付けたものを含みます）、プッシュ通知の購読情報、通知の予約、ログインの状態を、直ちに削除します。ただし、障害に備えた控え（バックアップ）等に、一定期間残る場合があります。</p>\n<p>4. 前項にかかわらず、個人を特定できない形に加工された統計データは、退会後も保持・利用されることがあります。</p>\n<p>5. ユーザーは、自己の情報の開示・訂正・削除等を希望する場合、第10条のお問い合わせ先に連絡することができます。運営者は、本人からの請求であることを確認のうえ、法令に従い、合理的な範囲で対応します。</p>\n<h2>第9条（未成年のユーザー）</h2>\n<p>1. 未成年のユーザーは、親権者など法定代理人の同意を得たうえで、本サービスを利用してください（利用規約第3条）。</p>\n<p>2. 16歳未満のユーザー本人またはその法定代理人は、前条第5項の方法により、ユーザーの情報の削除等を請求することができます。</p>\n<h2>第10条（お問い合わせ先）</h2>\n<p>本ポリシーに関するお問い合わせ、および個人情報の取り扱いに関するご請求は、以下までご連絡ください。</p>\n<p>運営者：森の主 OH-GY</p>\n<p>連絡先：ohgy.dev@gmail.com</p>\n<h2>第11条（本ポリシーの変更）</h2>\n<p>1. 運営者は、必要と判断した場合、本ポリシーを変更することがあります。重要な変更を行う場合は、本サービス上で告知します。</p>\n<p>2. 変更後の本ポリシーは、本サービス上に表示された時点から効力を生じるものとします。</p>\n<p>制定日：2026年6月27日</p>\n<p>改定日：2026年9月24日</p>\n<p>以上</p>'


@app.get("/terms")
def get_terms():
    return HTMLResponse(_legal_page("利用規約", TERMS_BODY_HTML))


@app.get("/privacy")
def get_privacy():
    return HTMLResponse(_legal_page("プライバシーポリシー", PRIVACY_BODY_HTML))


SW_JS = r"""// Service Worker。
// 役割はプッシュ通知の受け取りだけにしてある。
// 以前はオフライン表示のために通信を横取りしていたが、APIの応答まで
// キャッシュしてしまい「種をまいても一覧に出ない」不具合を起こしたため撤去した。
// オフライン対応が必要になったら、取り違えの起きない形で作り直す。

self.addEventListener('install', function (event) {
    self.skipWaiting();
});

self.addEventListener('activate', function (event) {
    // 以前の版が残したキャッシュを全部捨てる（古い応答が返るのを止める）
    event.waitUntil(
        caches.keys().then(function (names) {
            return Promise.all(names.map(function (n) { return caches.delete(n); }));
        }).then(function () { return self.clients.claim(); })
    );
});

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
async def websocket_endpoint(websocket: WebSocket, group_id: int, token: str = ""):
    """チームの更新通知を受け取る接続。

    認証: 接続時にトークンを受け取り、本当にそのグループの一員かを確かめる。
    確認しないと、グループ番号を順に指定するだけで、他人のチームの
    活動タイミングを外から覗けてしまう。
    """
    db = SessionLocal()
    try:
        user = user_from_token(db, token)
        if not user or user.group_id != group_id:
            await websocket.close(code=4401)  # 4401: 認証できない
            return
    finally:
        db.close()

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
    # 短すぎるパスワードを弾く（総当たりへの最低限の備え）。
    if len(password) < 8:
        raise HTTPException(
            status_code=400, detail="パスワードは8文字以上にしてください"
        )
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
            signup_source=clean_source(user_data.get("src")),
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
        "token": create_session(db, new_user),
    }


@app.post("/users/login")
def login_user(request: Request, login_data: dict, db: Session = Depends(get_db)):
    email = (login_data.get("email") or "").strip()
    password = login_data.get("password") or ""
    if not email or not password:
        raise HTTPException(
            status_code=400, detail="メールアドレスとパスワードを入力してください"
        )

    # 総当たり対策: 同じ相手からの連続した失敗を制限する。
    check_login_rate(request, email)

    user = db.query(User).filter(User.email == email).first()
    if not user or not bcrypt.checkpw(
        password.encode("utf-8"), user.hashed_password.encode("utf-8")
    ):
        record_login_failure(request, email)
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # 認証: 端末ごとに新しいセッションを作る（他の端末のログインは維持される）。
    clear_login_failures(request, email)
    token = create_session(db, user)
    return {
        "user": {"id": user.id, "name": user.name, "goal": user.goal},
        "token": token,
    }


# 認証: ログアウト。サーバー側のトークンを無効化する。
@app.post("/users/logout")
def logout_user(
    authorization: Optional[str] = Header(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    # ログアウトした端末のセッションだけを消す。他の端末は維持される。
    if authorization and authorization.startswith("Bearer "):
        token = authorization[len("Bearer "):]
        db.query(AuthToken).filter(
            AuthToken.token_hash == hash_token(token)
        ).delete(synchronize_session=False)
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
    new_goal = goal_data["goal"]
    # 目標が変わった場合のみグループを再編成する。期日(target_date)だけの
    # 変更ではグループを維持する(同じ目標の仲間と並走を続けられるように)。
    goal_changed = (new_goal != old_goal)
    user.goal = new_goal
    user.target_date = goal_data.get("target_date")
    if goal_changed:
        user.group_id = None
        user.strike_count = 0
        db.commit()
        if old_group_id:
            adjust_group_members(db, old_group_id, old_goal)
            await manager.broadcast_to_group(old_group_id, "update")
        assign_group_logic(db, user)
        if user.group_id:
            await manager.broadcast_to_group(user.group_id, "update")
    else:
        db.commit()
        # 期日だけ変わった場合も、チームメイトの画面に反映する。
        if user.group_id:
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


@app.post("/users/me/withdraw")
def withdraw_user(
    request: Request,
    data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """退会（アカウントの削除）。

    利用規約・プライバシーポリシーで約束している「退会」を実現する。
    本人確認のため、パスワードの再入力を求める（ログイン中の端末を
    他人に触られても、それだけでは消せないようにするため）。

    消すもの: 本人の学習記録・参考書・種・投稿・スタンプ・通知の宛先・
    通知予約・セッション、そして本人のアカウントそのもの。
    本人の投稿に他の人が付けたスタンプも、投稿と一緒に消える。
    抜けたあとのチームは、残った人のために3人に整え直す。
    """
    password = data.get("password") or ""
    check_login_rate(request, current_user.email)
    if not password or not bcrypt.checkpw(
        password.encode("utf-8"), current_user.hashed_password.encode("utf-8")
    ):
        record_login_failure(request, current_user.email)
        # 401 は画面側で「ログイン切れ」として扱われ、強制ログアウトになるため 400 で返す
        raise HTTPException(status_code=400, detail="パスワードが違います")
    clear_login_failures(request, current_user.email)

    uid = current_user.id
    group_id = current_user.group_id

    # 外部キーでつながっているものから順に消す（親を先に消すと失敗するため）
    my_message_ids = [mid for (mid,) in db.query(Message.id).filter(Message.user_id == uid)]
    if my_message_ids:
        db.query(Reaction).filter(Reaction.message_id.in_(my_message_ids)).delete(
            synchronize_session=False)
    db.query(Reaction).filter(Reaction.user_id == uid).delete(synchronize_session=False)
    db.query(Message).filter(Message.user_id == uid).delete(synchronize_session=False)
    db.query(Report).filter(Report.user_id == uid).delete(synchronize_session=False)
    db.query(Book).filter(Book.user_id == uid).delete(synchronize_session=False)
    db.query(DailyGoal).filter(DailyGoal.user_id == uid).delete(synchronize_session=False)
    db.query(PushSubscription).filter(PushSubscription.user_id == uid).delete(
        synchronize_session=False)
    db.query(PomodoroAlarm).filter(PomodoroAlarm.user_id == uid).delete(
        synchronize_session=False)
    db.query(AuthToken).filter(AuthToken.user_id == uid).delete(synchronize_session=False)
    db.delete(current_user)
    db.commit()

    # 残ったチームを整える。人間が残っていれば3人に補い、
    # AIだけになったら解散させる（AIだけのチームに意味はないため）。
    if group_id:
        grp = db.query(Group).filter(Group.id == group_id).first()
        if grp:
            humans = db.query(User).filter(
                User.group_id == group_id, User.is_ai == False).count()  # noqa: E712
            if humans > 0:
                adjust_group_members(db, group_id, grp.goal)
            else:
                cleanup_ai_only_groups(db)
    return {"message": "withdrawn"}


@app.post("/juku/join")
def join_juku(
    data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """塾コードを入力して塾に所属する。

    所属すると、次のチーム編成から「同じ塾の生徒」だけで組まれる。
    すでにチームがある場合は、いったん外れて塾内で組み直す。
    """
    code = (data.get("code") or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="塾コードを入力してください")
    juku = db.query(Juku).filter(Juku.code == code).first()
    if not juku:
        raise HTTPException(status_code=404, detail="その塾コードは見つかりませんでした")
    if current_user.juku_id == juku.id:
        return {"message": "already joined", "juku_name": juku.name}

    old_group_id = current_user.group_id
    current_user.juku_id = juku.id
    current_user.group_id = None
    db.commit()
    # 抜けたあとの元グループの人数を整える
    if old_group_id:
        old = db.query(Group).filter(Group.id == old_group_id).first()
        if old:
            adjust_group_members(db, old.id, old.goal)
    assign_group_logic(db, current_user)
    return {"message": "joined", "juku_name": juku.name}


@app.post("/juku/leave")
def leave_juku(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """塾への共有をやめる。学習記録は本人のものとして残る。"""
    if not current_user.juku_id:
        return {"message": "not joined"}
    old_group_id = current_user.group_id
    current_user.juku_id = None
    current_user.group_id = None
    db.commit()
    if old_group_id:
        old = db.query(Group).filter(Group.id == old_group_id).first()
        if old:
            adjust_group_members(db, old.id, old.goal)
    assign_group_logic(db, current_user)
    return {"message": "left"}


@app.post("/pomodoro/alarm")
def schedule_pomodoro_alarm(
    data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """ポモドーロ完了通知を予約する（既存の予約は置き換え）。
    アプリ側の鐘と二重に鳴らさないため、期限には5秒の猶予を足す。
    アプリが生きていれば端末の鐘が先に鳴り、その場で予約が取り消される。"""
    seconds = data.get("seconds_from_now")
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise HTTPException(
            status_code=400, detail="seconds_from_now を数値で指定してください"
        )
    if not (1 <= seconds <= 2 * 60 * 60):
        raise HTTPException(
            status_code=400, detail="seconds_from_now は1〜7200の範囲で指定してください"
        )
    db.query(PomodoroAlarm).filter(
        PomodoroAlarm.user_id == current_user.id
    ).delete(synchronize_session=False)
    db.add(
        PomodoroAlarm(
            user_id=current_user.id,
            notify_at=utcnow() + timedelta(seconds=int(seconds) + 5),
        )
    )
    db.commit()
    return {"message": "scheduled"}


@app.delete("/pomodoro/alarm")
def cancel_pomodoro_alarm(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """ポモドーロ完了通知の予約を取り消す（未予約なら何もしない）。"""
    db.query(PomodoroAlarm).filter(
        PomodoroAlarm.user_id == current_user.id
    ).delete(synchronize_session=False)
    db.commit()
    return {"message": "canceled"}


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
        # アイコンは「人」か「AI」かだけで決める。以前は学習時間が多い人に別の
        # 印を付けていたが、チームメイトの間に差を見せることになるためやめた
        # （みんスタは比べない）。AIは人のふりをさせないよう区別がつく印にする。
        if m.is_ai:
            total_mins = 0
            icon = "ai"
        else:
            total_mins = totals.get(m.id, 0)
            icon = "person"
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

    # 入力検証: APIを直接叩かれた場合に備え、欠落・型違い・範囲外を弾く。
    # 検証しないと、マイナスや極端な分数がそのまま記録され、
    # 森の成長・グラフ・芝生といった集計がすべて壊れる。
    content = (report_data.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="学習内容を入力してください")
    if len(content) > 500:
        raise HTTPException(status_code=400, detail="学習内容は500文字以内にしてください")
    minutes = report_data.get("study_minutes")
    if isinstance(minutes, bool) or not isinstance(minutes, (int, float)):
        raise HTTPException(status_code=400, detail="学習時間を数値で入力してください")
    minutes = int(minutes)
    if not (1 <= minutes <= 1440):
        raise HTTPException(
            status_code=400, detail="学習時間は1〜1440分の範囲で入力してください"
        )

    r = Report(
        user_id=user.id,
        book_id=report_data.get("book_id"),
        content=content,
        study_minutes=minutes,
    )
    db.add(r)
    user.strike_count = 0
    db.commit()

    if user.group_id:
        msg = Message(
            group_id=user.group_id,
            user_id=user.id,
            content=f"【学習記録】{content} ({minutes}分)",
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
            # UTCであることを明示して返す（+00:00 付き）。印がないと、
            # ブラウザが日本時間だと勘違いして9時間ずれた時刻を表示する。
            # 秒までに丸めるのは、古いSafariが小数点以下6桁を読めないため。
            "reported_at": r.reported_at.replace(tzinfo=timezone.utc, microsecond=0).isoformat(),
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