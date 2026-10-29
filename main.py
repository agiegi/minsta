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

# Googleログイン用のクライアントID。これは秘密ではなく、画面にも渡す。
# IDトークン方式なので、クライアントシークレットは不要（管理する秘密を増やさない）。
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")

# google-auth が入っていない環境でもアプリは起動させる（Googleログインだけ使えない）。
# これが無いと、ライブラリの入れ忘れでサービス全体が止まる。
try:
    from google.auth.transport import requests as google_requests
    from google.oauth2 import id_token as google_id_token
except ImportError:
    google_requests = None
    google_id_token = None

GOOGLE_LOGIN_ENABLED = bool(GOOGLE_CLIENT_ID) and google_id_token is not None


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
    # Googleログインで本人を見分けるための、Google側の利用者ID（sub）。
    # 結びつけの軸はメールアドレスではなくこちら。メールは変わりうるが sub は変わらない。
    # Googleだけで登録した人は hashed_password が NULL になるため、
    # パスワードを照合する場所では必ず NULL を確かめること。
    google_sub = Column(String, unique=True, index=True, nullable=True)
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

    # Googleログイン用の列。既存の利用者は NULL（＝これまで通りパスワードでログイン）。
    # 一意索引を張るのは、1つのGoogleアカウントが2つの利用者に結びつくのを防ぐため。
    # NULL は何行あっても重複とみなされないので、パスワードの人には影響しない。
    if "google_sub" not in cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN google_sub VARCHAR"))
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_google_sub ON users (google_sub)"))
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


# ===== 入力の検証 =====
# 画面からは起こらない値でも、APIを直接叩けば何でも送れる。
# 上限が無いと、1人の利用者がデータベースを埋め尽くせてしまう。
# また dict["key"] の形で取り出すと、キーが無いだけで500になる。
# ここを通して、足りない・長すぎる・形が違うものは400で返す。

MAX_GOAL = 50           # 目標。チーム編成の見出しにもなる
MAX_NAME = 20           # ニックネーム
MAX_BOOK_TITLE = 200    # 参考書の題名
MAX_EMOJI = 16          # スタンプ。絵文字1〜数文字で足りる
MAX_DATE = 10           # YYYY-MM-DD
MAX_PUSH_FIELD = 512    # プッシュ通知の宛先と鍵
# 画像は data URI で届くので、元の画像のおよそ4/3の大きさになる。
MAX_PROFILE_IMAGE = 200_000   # 100×100のJPEGなので十分な余裕
MAX_COVER_IMAGE = 700_000     # 表紙画像
MAX_BOOKS_PER_USER = 300      # 1人が持てる参考書の数
MAX_SEARCH_QUERY = 200        # 書籍検索の語
DEFAULT_BOOK_COLOR = "#66BB6A"


def need_text(value, label, max_len, required=True):
    """文字列を受け取り、型と長さを確かめて返す。合わなければ400。"""
    if value is None:
        value = ""
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail=f"{label}の形式が正しくありません")
    value = value.strip()
    if required and not value:
        raise HTTPException(status_code=400, detail=f"{label}を入力してください")
    if len(value) > max_len:
        raise HTTPException(
            status_code=400, detail=f"{label}は{max_len}文字以内にしてください"
        )
    return value


# プロフィール画像は、画面側で必ず100×100のJPEGに変換してから送っている。
# そのため data URI だけを受け付ける。外部のURLを許すと、画像を置いた相手に
# 「チームメイトがいつ画面を開いたか」がIPアドレスごと知られてしまう。
DATA_IMAGE_RE = re.compile(r"^data:image/(png|jpeg|jpg|gif|webp|svg\+xml)[;,]")
HTTP_IMAGE_RE = re.compile(r"^https?://[^\s\"'<>]+$")


def need_image(value, label, max_len, allow_url=False):
    """画像を受け取る。空なら None。大きすぎる・形が違うものは400。

    allow_url=True のときだけ、http(s) のURLも許す
    （参考書の表紙は、Google ブックスの画像URLをそのまま持っているため）。
    """
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail=f"{label}の形式が正しくありません")
    if len(value) > max_len:
        raise HTTPException(
            status_code=400,
            detail=f"{label}が大きすぎます。小さい画像を選んでください",
        )
    if DATA_IMAGE_RE.match(value):
        return value
    if allow_url and HTTP_IMAGE_RE.match(value):
        return value
    raise HTTPException(status_code=400, detail=f"{label}の形式が正しくありません")


def need_color(value):
    """参考書の色。#rrggbb だけを受け取る。
    画面では style に直接差し込むので、ここを緩めるとCSSに細工できてしまう。"""
    if value is None or value == "":
        return DEFAULT_BOOK_COLOR
    if not isinstance(value, str) or not re.fullmatch(r"#[0-9A-Fa-f]{6}", value.strip()):
        raise HTTPException(status_code=400, detail="色の形式が正しくありません")
    return value.strip()


def need_date(value, label):
    """YYYY-MM-DD だけを受け取る。空なら None。"""
    if value is None or value == "":
        return None
    value = need_text(value, label, MAX_DATE)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise HTTPException(status_code=400, detail=f"{label}は YYYY-MM-DD の形式で入力してください")
    return value


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


def _ip_key(request: Request) -> str:
    ip = request.client.host if request and request.client else "unknown"
    return f"ip:{ip}"


def _check_failure_keys(keys: List[str]):
    """与えられた鍵のどれかで失敗が続いているならロックする。"""
    now = utcnow()
    for key in keys:
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


def check_login_rate(request: Request, email: str):
    """失敗が続いているならロックする。ログイン処理の最初に呼ぶ。"""
    _check_failure_keys(_login_keys(request, email))


def check_ip_rate(request: Request):
    """IPアドレスだけで数える版。Googleログインに使う。
    トークンを検証するまでメールアドレスが分からないため、IPしか見られない。

    鍵をパスワードログインと分けている（gip:）のは、巻き添えを避けるため。
    同じ回線の誰かがパスワードを間違え続けても、Googleでのログインは止めない。
    Googleログインは総当たりの的ではない（正しいトークンがなければ通らない）ので、
    ここでの上限は、でたらめなトークンを大量に送られたときの負荷を抑えるためのもの。
    """
    _check_failure_keys([f"g{_ip_key(request)}"])


def record_login_failure(request: Request, email: str):
    """失敗を記録する。"""
    now = utcnow()
    for key in _login_keys(request, email):
        _login_failures.setdefault(key, []).append(now)


def record_ip_failure(request: Request):
    """Googleログインの失敗を、IPアドレス単位で記録する。"""
    _login_failures.setdefault(f"g{_ip_key(request)}", []).append(utcnow())


# ===== 登録の連打を防ぐ =====
# 上限が無いと、自動で何千個もアカウントを作られ、チームとAIメンバーが際限なく増える。
# 塾で生徒がまとめて登録する場面があるので、1クラス分は通る広さにしてある。
# 足りなければ Render の環境変数 REGISTER_MAX_PER_HOUR で変えられる。
REGISTER_MAX_PER_HOUR = int(os.environ.get("REGISTER_MAX_PER_HOUR", "30"))
REGISTER_WINDOW_SECONDS = 3600

_register_times: Dict[str, List[datetime]] = {}


def check_register_rate(request: Request):
    now = utcnow()
    key = _ip_key(request)
    recent = [
        t for t in _register_times.get(key, [])
        if (now - t).total_seconds() < REGISTER_WINDOW_SECONDS
    ]
    _register_times[key] = recent
    if len(recent) >= REGISTER_MAX_PER_HOUR:
        raise HTTPException(
            status_code=429,
            detail="登録の受け付けを一時的に制限しています。時間をおいてお試しください",
        )


def record_register(request: Request):
    _register_times.setdefault(_ip_key(request), []).append(utcnow())


# ===== プッシュ通知の宛先の制限 =====
# 宛先URLは利用者の端末から送られてくるが、サーバーがそのURLへ接続しに行くため、
# 制限しないと「内部のアドレスを登録して、サーバーに代わりに叩かせる」ことができる。
# 攻撃する側に応答は返らないが、内部の様子を探る足がかりになる。
# ブラウザのプッシュ配信元は数が限られているので、その一覧だけを許す。
# 新しいブラウザが増えたらここに足す（拒否したホスト名はログに出る）。
PUSH_HOST_SUFFIXES = (
    "fcm.googleapis.com",          # Chrome / Android
    "android.googleapis.com",      # 古いChrome
    "push.services.mozilla.com",   # Firefox
    "notify.windows.com",          # Edge / Windows
    "push.apple.com",              # Safari / iOS / macOS
)


def clean_push_endpoint(value):
    """プッシュの宛先を確かめる。知っている配信元のhttpsだけを通す。"""
    endpoint = need_text(value, "通知の宛先", MAX_PUSH_FIELD)
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.scheme != "https" or not parsed.hostname:
        raise HTTPException(status_code=400, detail="通知の宛先が不正です")
    host = parsed.hostname.lower()
    if not any(host == s or host.endswith("." + s) for s in PUSH_HOST_SUFFIXES):
        print(f"push: 未知の配信元を拒否しました host={host}")
        raise HTTPException(
            status_code=400, detail="このブラウザの通知にはまだ対応していません"
        )
    return endpoint


# ===== Googleログイン =====
def verify_google_credential(credential: str) -> dict:
    """GoogleのIDトークンを検証し、中身（sub・emailなど）を返す。駄目なら 400。

    署名・aud（宛先がこのアプリか）・iss（発行元がGoogleか）・exp（期限）は
    ライブラリが確認する。email_verified はここで確かめる。

    画面から送られてきたメールアドレスや名前は、本人確認には一切使わない。
    本人を示す情報は、必ずこの関数が返した中身から取ること。

    検証を関数として切り出しているのは、試験のときに差し替えて
    「期限切れ」「宛先違い」などを、本物のGoogleに繋がずに試せるようにするため。
    """
    if not GOOGLE_LOGIN_ENABLED:
        raise HTTPException(status_code=400, detail="Googleログインは利用できません")
    if not credential or not isinstance(credential, str):
        raise HTTPException(status_code=400, detail="Googleの認証情報がありません")
    try:
        info = google_id_token.verify_oauth2_token(
            credential, google_requests.Request(), GOOGLE_CLIENT_ID
        )
    except Exception as e:
        print(f"google verify failed: {e}")
        raise HTTPException(status_code=400, detail="Googleの確認に失敗しました")
    # 未確認のメールアドレスを通すと、他人のメールアドレスを登録しただけの
    # Googleアカウントで、既存の利用者に成り代われてしまう。
    if not info.get("email_verified"):
        raise HTTPException(
            status_code=400,
            detail="Googleアカウントのメールアドレスが確認されていません",
        )
    if not info.get("sub") or not info.get("email"):
        raise HTTPException(status_code=400, detail="Googleの確認に失敗しました")
    return info


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
# 画面とAPIは同じ生成元で配信しているため（user.html の API_URL は空文字＝相対パス）、
# 普段このリストは使われない。別の場所からAPIを呼ぶときのための備え。
# みんスタは2つのアドレスで見られるので、どちらも載せておく。
ALLOWED_ORIGINS = [
    "https://minsta.oh-gy.com",
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


# ===== 安全のためのHTTPヘッダー =====
# ブラウザに「このページで何を許すか」を伝える。
# とくに frame-ancestors は、みんスタを他人のページの中に透明に埋め込んで、
# 利用者に気づかれないまま退会や卒業のボタンを押させる手口を防ぐ。
#
# 許可している外部の行き先は、実際に使っているものだけ：
#   accounts.google.com  Googleログイン
#   cdn.jsdelivr.net     グラフの描画（Chart.js）
#   fonts.googleapis.com / fonts.gstatic.com   表示に使う文字
#   assets.mixkit.co / wikimedia               環境音
#   data: と https:（画像）  プロフィール画像と参考書の表紙
#
# script と style に 'unsafe-inline' が要るのは、この画面が1枚のHTMLに
# スクリプトと見た目を直接書いている作りのため。外部ファイルに分ければ外せる。
CSP = "; ".join([
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://accounts.google.com",
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://accounts.google.com",
    "font-src 'self' https://fonts.gstatic.com",
    "img-src 'self' data: https: http:",
    "media-src 'self' https://assets.mixkit.co https://commons.wikimedia.org https://upload.wikimedia.org",
    "connect-src 'self' https://accounts.google.com",
    "frame-src https://accounts.google.com",
    "frame-ancestors 'none'",        # 他人のページに埋め込ませない
    "base-uri 'none'",               # <base>で読み込み先をすり替えさせない
    "form-action 'self'",
    "object-src 'none'",
])

SECURITY_HEADERS = {
    "Content-Security-Policy": CSP,
    "X-Frame-Options": "DENY",            # 古いブラウザ向けの同じ備え
    "X-Content-Type-Options": "nosniff",  # 中身を見て勝手に種類を決めさせない
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
    # Googleログインは別窓を開くので、同一生成元に閉じきらず別窓だけ許す
    "Cross-Origin-Opener-Policy": "same-origin-allow-popups",
    # 一度HTTPSで来た相手には、以後HTTPで繋がせない（1年）
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
}


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    for key, value in SECURITY_HEADERS.items():
        response.headers.setdefault(key, value)
    return response


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
LOGO_WEBP_B64 = "UklGRnCLAABXRUJQVlA4WAoAAAAQAAAA8wEAlwAAQUxQSDVHAAAB/yckSPD/eGtEpO4TENy2DSRZdmd355L5/4ObmVvcfSCi/xOg2/LxP+9kJ+d/ANiAzALQfdA3+dykClgrsBrtBdgPfNMAN+kq7Z4+/mCRTeBHyWB2D7kAtrW2lxeAbqu6bW9n9ySgu7dEUDfrx9gJ8J6oQJWGizVJSSIpSaMkA0BVYpNISjgaXFjKmVqekpckXRUtqIndtWdwcvZhF8mZpMonkpJzrbUkkPJhLUk1uao02GQGuyK7Y2VnkJJk9SnJxhufj8a1IPJNR2tBopq4e2m3YbM9PV9KJG2LJDYDSTJogGKtL/z6wdO3D6pewOtbSGJ6SD8hqff+yOfUEqh76CF6/gZfW1VPcetxVbUsiW53uZJMhH7zdJK5ij21+CnqxQXvpdPUBWGUkvyO73c3v1V/gT8tEQaEtpEESTF/2NPTc3f/ACJiAtrqbxpLf/j4q1Cy8QMeVCzWsnPD1KF306PFUl0wqPRdmyXaM3jT3gQtJZVOL6wKvWrt5tCzcaPKglKq0Kg8cEtcVKnQaNi/fdn4UIcdqawMpb3KfNNnb1dvfUIZNPpA6fmiGBzKIQPT/+v+FRd9/P8/kpR4r/f3W9UTNy+bYFnikkEUATlAzGCOmO4nBszpjGcOZw6YcwCzYkKMeEYEBZGcYWGBzTnNzkyHqu/7j67umekZvB//RcQE+I0kybJt27bMss0/tj4E/NdCJ7rwL6OLjQUtehY8P32UAxkRmW1MBiJiApghpTT0kvV/3inMA+ehfNxln7rgsldaPHCuNPzJ4xu1o7707JQ9kHbYgc1Mo7xEPIDG0kFLgR0OD5hJSIC4k/iAGTCbymMDkxn0AJTaaiDEwQNF6E4JmPDAE5BxLpU2p8IcNER4ICryOAcwu+tm4rZHf2HVtQ9BD0AFDt0ZDbQSIxQ4+FGLj71qz+sJD0QdlGirM7bNUGy2WrXzmmc4POAkFu4OANtZDjCX7gwQU/G/T3kACnFPMnJk7LB5WzAQ4qv7eAA6cm0oUksbWQNr3ZXBNrBsuR/w0F+A9MVbhvqG13xVzwD+5SgQyxcS/q1SjPq/VhACAzLG4F5A3v+th63+wk3BGBCE8ti/91uQxoeZYYVA2J6RFFQAUf/OSBIxzBSSSGaCijiBp4yQkAmmgxS+9bx6Bpo/jGYGIYSwbdqDkmceZwXznjD3ZxvRvy+KBVCmoBlAkQIgP3DZ7AP7a2E9O/Zs39rYCoSQ0tQRlKITgBHE4sGXB4VCYzkzoYIo6BxrcwYiO3ZASDNOSkc/9VnHcvtPZqN/V4TzFUc0Dh90mm6KlNB/2DGHHb30wPmKVDbGtxerrrjz9ptBUVMlAGE6Zq0XXjCauX/PXtA0k0QJ5PPnDh4yd+6CJQuGB/cZiGy//dffa4SZRjzvU4upUwuXLpb+PRF66YsPXJB+/bMXBWkaKQZg9tMeetIRQ0AqnQAhERRg1x/++JdVEDVFkmwiyW4L5ZGXLoRLb8VmOks2sOyolStOWTo/H6JjggCXv+qmMMOQfbtsxoBbvpB/UwIfeAdlof7DXvmSaaQI7PeiMx5yFKTCSEJGYDC2YmTnpY+NECdPCqE0SAlCdAJUPuFNc371KcS0jsCyJUef8MQVAUgpGYSQhe2+DY8hzCQma+GAwI3mY4n/jsTyMb8MKQhazRNqKUwTweBTvrXG0EQSk2kSWSj+8oohQpgcSRnEk4960OLizhuvW1cnTwZSyBt4WimQnfT602YNQSshIUB02+r/44A0c4jrmrEICKDh36J/PxTU/5eTmgFQIC66h+lae/HVpVMDBabQpJZ9+VkQb0gKzP7PZ50wJIAt9/zm22vIEoTkYKaT4Ox/HVujLB2E6Fauoqw9ijhjpLDq5uCIqEjnEWYuTZ/IG89rRBCSon9TaJqc8ZDQaCrSW2W2J9eKH/7PXWhCJognvvR7fnoQQg5bvvmBcYAESXSUey6472Ov5+MRge5EZ/vBzJjW3l8CCBBujZ84cwiEOsiysaeHIgf//4sg9Y6MafrY8ZaDQAggw5xAqXzTi0iaCIbFD+IHF4LYrvGbF4MrkQwIMBKhdyTe+/qmVoTcQwHNGFy8JZpKQzFywjSTQICNmcTMmibhS69HaodricX0eG4uUW1og1dwq79+Up+70S3ML5t6AUjpov+GIQxtIMAQQiqBENQjoTzlzY0Q2MiRO4iNzJQOt/wpS4Bpt++Yg6aHCGCbLvsHhrJ8wT6zSw3uGqhv6r97d2MUwjRQxlnNRetY/zny9Chq7oSpwl5ZBIFxGQYOm9UVWGk0Etilbw5s2hKSTWcFl7B00ZwjQeoN8cS8LtrI6qJLwzY8Q4gv7omms/x2ItNQkYIEsHBBnDMw9/S4aL7mzZoVs8E+quvZ1pHmD/4wROg5hTD/H+npUu2HIRQK7k5WT6zNExM3BCEgYxsnjpjVHfx2LCARmbpk9SimY3QBpx1/9CNnD277+hxJvSBzjEX3BowJAiSAfOeqmUK66urMIKki27QITYMIxKWHHbl4ybKVC1zLaqLSJiWQZYIzwfhNT0c9l/F21+nFz5WVMoA7pZjFGDRlVwtcYbDCAHJuIMDOXaFKIN74g8x0qStwrJ9PpaXQYPkJz3jaIKSy4dcSUQ8gdopunUwIkfYSRLt17zpmyKz19rGIqJZ5N4Hej8w+89lHBtOeZDtVCECi3RbGwcW7YuixqDN2N1uAAqnvB6KArJRccxqIlE25CaBMU3T37shk3wDTvnUnBhXlXRvybqRCuMy+eHVMVBZnnLNyPzVLBTFWXEAUPWj4A65KsjLAIzvuGru5fO1sd+KG8ZhmBMWrL4mJzmX8g4J6L/LYOxJQlEAAENUGMUE3mj6H2FuBP7heIChL5bzPDtSGlLeIDJXNOVkoxmKx8Uv/9YyjBFFTctcvY1mVI5zEqkpxFQEFWJtfRbcGQBZ27c730yb81jMoCgcJea/fTaQnS1107YPrWoEvFrnv1tvuW7Vux5YCLnhhMwpX3ISYCZXx7SLQZdBniPR8iI/Z4DpSHphs4U4UjeL25VIvRd6cGkUBEFDpSQuYVNs7L3nNMghTUVxuuswmIBDALQawMpi0jkBl4BilDkotIA48FAGB5evKkboAm8BLDytDbxA4+lc2QP2v/3vtzdtpV8xrywEbIHAnnhFCqP3vi0+NXfL0ftPzMT3rt4vrGVMpuiKWhz3HoXckLhvPEEDY80SIEASrGGWBtZ8ZIGjyuEvq4jiAhF2GTvntf01VYikdldIUKd+3w5HbxkfHIOaCvbuW07uB8PJfHNS6+6q/XAMoyMZqHXSSA5XOtlxLmgmU8cpHaqHyOStSVK8pzf6vrJkxVXiAPUAPB11/QzBdB9kDIFOT3MefD1eYPF16R1Z2MrMARoDICOvuUmqjK3YjpZlHe9DQJbahvmX1fddeewW9HAWzWnUIwckAAh4yXKgq6Z8b5Akoxhh6L7DPPV88ncqhd+Wm54OfeVIjo0t5YqJjKukguWcUfSem2uJcIFt7mfwjpiDu+BrYRgCDB3DBBlCrIrK5EyBAAyMVCiw/7+Lvzl194T1bSlCQegdlIRFDKunYdhKm2lxFoGsF2hV6LepLfujwYQNJ6rkye7npWnhCoIlwTEzqlZyf4uQqvCFlQAZIzT0PIkya9fN7MtNZphV4BDg7YTcCAjd1IYDswXF0NQZQoKNCcKLHJQzuQuXgiahD4Gq6F6x42ptfcBKE3oqc1WwGLELroU9BpudDeY/cnam0DciSQikBoTbXFcE9Ejh5dTCdDgMOwMKA6/4ccdJSuPcilV0AEbLNt2AjpbmNXTKIK8czV0EADEDS3auqUIghEGzTrt5ClulSwfsd6g7Odt2Fu5Gyj2+3Xf/uStRLCv1/c5NG8KpBi96X/2y7G4xtlAWqS4/NpmR+PaI3xdzbMtNdbMIugFu729LFhElD+sbGLHUTAuYINxAbIMV519Oum29RB5mm5JKxrGqCNtNakUfPK4KqdPe93QV9yanZqLe85hiFXlHK+C83S0GB0Hzw6SkwDc0l9+al22ySFCWArbtW71y9ey17Rrz7oR+ZbzvnG35J6gmFOPg7U2lbhOD2QIigsKwwrvtnU5Gy274e3U0ZLhoKJAnKxVsbEYjNSzoNkzz99e+AJmPaB84UoAr+XsRuIi/zWLMoitaov0oPBQ3enpolgCK118lMxxTWfC7rdyEUA+Adre2rtt12z027tpvOP39a8zXQzm30oqSc/3FDEoi28JAX8cXx02CavmkATZZRGno4kywRYjZApopvHiwFoTzwhagb2+SCvR/+SLQ944S03/EWnf9Jt0r95zoKUJ4eOS+pV4itsDCJjs52fhpNCwi1t6wqDGnX2M6br1m1amznHtpdblJ58CLHNIafKpY9AOaacZlql2WRCtu7Q62oj6YNam3bPNrob+zd2Vj2gkOTqqDpKzImX4Ty5IcVAU0CCBBKiaN05NEOAD5yNsmAcHLMYPMdq2/6y3VZwjIWnkHg4CVFAGRc234bnni/FUm0qzzoQfSMuP2gYDqqzL9CLKaJxD6PfcusravvGdm9h/Yo+QltSKfSygf57l8hevPuPTHRLnDR8OfPePHznramOdTaM5b2uNWi8xdenWKnhn9MmDwg8eBaAk/MFJANBGLAoGLgibhNWIgAyX2Mb7visitubsBAKp3AEUFCM8ZjswYGRIp/QeUg8KCFpdRGCsf2ThgfWFBKHVJt4y9CYtoGOkdhbCAAAirmnGsUi6BbBlT2gBzuWx3sTjT8+0G6VxBIkBY8HlFtmn4+cSoM7IOF2swAOQu9ePysUgBm/O8pNKy+dNPFl9+1plRGi/ZgZwXtilnUDKA063HIol38hpAGsCKYzqJXI2tWiC4dzyMrNG1QjIAxpvuiPIdmX6J1+AaxF4JG7sJ0ECqvXk4WYzQyYKpFLM5cUXSR0s2z0VSAPOeRTNhMSIGM036Lwe3wl4vfsZx03fd/uCkEChg46KDFBy5cOMsMbxxdv+GO2zcARE0/jji8ENXZ7rsw4yHUxSE9k++cO78Urkq1+/6kJGZeQcXQS2jPZvKNN5KYemXcgugylOnxZEyiUOKZIUWrzZT+BoEpUjp8n1KoGznMwEblvIMR1ZFFj11x3T929Wu8b/6DTt//4EPn03XafO8Xzzm+BmH6nTTUiKpwWF+fyGxQhWAu7o0Q1u4vgarQF4jFDBXKs2n225RJ9hVC2QM5W4w7CeuzIdPkhPKUR6YojAxW/bSpQwztG4UhFd6Qeao9io6yCqCWijlPesqxK/qgLI0QYINy2Xv+/Py5hOklcwadU7i3lpWQSokj6SwGeiVjdE4SuCLlt/wtlGJGspz1Skumfr6648+hZOojy7fmxl1wZB7CJOEXDbYiYJDL2p9i0BQBZmUdUymROCODNHAkqUIChaxVHPKyM4+BZqkggag22PV60+mqhxKmjQJQrjyjDB1C6wLaXUCCyLqAMdQTkZUrEZ0dvkNfAguiZphX8TpafSXVcb2XmKZMkf3+ILp08MpDySRNRixOfWYZ6GiHs8imLja3bsMVgPRIIEhrxOzoCnBKrWKfs59/DD8/igAyD6IoVpx9J3E6SHIqgfwp8x9qZxveqc3Xj2gPlZGlK6wqm9XEXlDU9+emCiOn2u3flakuS6I0Uxj49Z5gOgqeWkP04Pg8go0oF+1H1hQgPKEr/xgTXej1NWvqALETVCEmO4zFfouQ1IYKHv28ZdRZhPoEEdSYddCg1HNBJTD7QUccfNAh+xbUgjOaa0bjmlU7Vm1dv35vWnxA6gBs6AllPDulQEcr/TdLFpx0SJo7Pp6evC8uw4yR4oabopFUlR69pBRTbYfGCKa3yv4zCQ2JQIECyV1ELikTHRWKxz2mjPQEYl4de0ogk0rRLnHwE4+kHgWN1HYICZbTYwolDBxy2mnHH5QDLisB28qpLMfXbbmnL7pTLG4jTJ2U8RMnusj/1vj8k5bMpfKdXzz/3LngmSFp+x9IABUq5r8ZMXVh7+VAGiFOnVsqEIpEfz4CZKWrpDnX1Iou4LWDpehRs3oLWZpAcgAoECDNX9IhcdR/qC5hjmUqnN8z3FvKgEOf8uwjhiC1QNImig0yJFAIAkrRMdu9LcYeyDhxTxGqJPDwm8BFanMz7vfYNz6EGdJcWQ+4E37wbE+dTQObpAikJQeXQmotffyjDuC+qy//125iqgh8WnanUJ6yfxLqEQV0096oJHdxVxDkobkVShy/T0oBJkF6AaTsEsVeirDo9HMetoCykGKkY9gNIDDtxgruYO67FWLUVMVwgRt0ltLysiEJDGRqNViSEWaCFK66M0+iTRBb/3FaikwdfxrLDFQoLT8IgYrHv4PK8rYvXriTKAgctSN2oXLWO+jpxNpv/43cpQGrXNglVN+DBQ4LB4sokJi4HRKo9Uay3gmBfd5+7wrKQgEBRiBtV6JaFmCUFn74rGEgRk2BMk5rNlpdgIqQ0VkKUTTuUpgBws5LSaZSAGdFM+VBd9ydpU5iSgdhm9aZ51IPyFaNO75yPsQQapc4YRswoTzz6CT3kFPkqivWExPS54KUMpJYcjBJICZ1AqTab84LolcDvHC13XIQ1TKgE8EEEICQl709Xf+JJy4CsjBZCmH23zwBJCZqLiRONyFuLgJ2B/GE2WWYumzH+wh0+4T5JDlp+C00hhPGJGpc9ax+Ml7nBoCNrGKfR1n0euSCH4/G9QFW91WWAJqLZTqad6j5X3dnvWLgiB/Y9WagWky6EROWi9Sy073nPXIehMnKOMuN1gQmMeU3zVeYZphrRqKhQyiWPo4eVPz5d/LUFkDl2EHAYe+DYmugpGNKff77k3nqWKvZhhHy2bN6TwoFx73k7MU0raBOwiCjLpQG5hHZ2yAh+gAOpFu5/+kyPRISz3gSzSA6C6RJMZOBvkhJNbjtbQemMDll+OPv8sREpYng8HxPM5vtO4KpFjT8Q2IviFlUVgRWHkSUlPGSy0vRbSLnuyetbAWEwcTiYY9OgZ4XkYJDXvCcQ4EiIYluExDcRsr3x611yMaNKigWc/GM/crQI2bW3GETRKWwTLs6JbcJBGhiiMFOIf/I5aemMClq/U8hOltBMkHdFPkRWNMqxbW3YZDaUOlVy0LohcAG2wIblBYdQKaKJ3+9EVUhDJBCpJDoaP2/gKYDBErmn/qw449dFMHJCQRGCgEKVZgVCiUSXSaAgHikcp/HAqYHFTjk4RSBzsJOVncZ1S5sSSIHbBjS+LLz90maBIVr/5K7ixR2XgOasYvUd1yeppfL9VS7zYw9kkyaOiKbZdptp4GlZGoPLPiEDCDaDaKU6JTS8UtKMV0jJbD08Iftf9zBs2NGx7K5aevQkYXaYJ5fbSFUIYcCCaCKMiyjRw3ahyS6dFKeZwZQm/EF96Zw+Ipy2bwFAspW5AoWef3Q5xAmpqz5LdqlAsR2Mjt/cc28Z+7fDOrg7ODlZvqKwGrkisqUv5a+3oCLm3kRwWUR4gJAtEfOOLCYkEF0PuBxiGmsoNJAftjiwYOXZcvz9TvGxq8fX7Nnzu+OKUJFPPOjMqKjJhkgvcCcHHolsXyYrp1ycd/F31yQu1Ncc94IgBbNW3nIyhXLFi92thwBCj4NTyzoV7dnSQJQSuf+1bs+CId+/bQiqArNXYimD2aPQR0EDucdDHlQD8Qb3//Bvga4P2zchekoWNjKMneYeFl7ZGCaC0kUTDTf8N1PGCD44Z+OpmMAlQywQyUHrUzqETb0o27KLIxedPGVHwyHFUEgIMW/hizhhAE0Z+njXvF6GAYBV4BaTFwZnxSmuuG6X91CVFj15sszU22V43j6BNaORtOtwJu/8SDIpkw4fvg171wGuuOH/wSpA5hrLi4zJDQRg7ORY8ow7dolCZMIAmPpiiJLAqXjaOEOtVwua3MHHXojxav/QaKznY/88rzrYMFLo0R18M/JSoPanex5V7Fbta7y0MQUQrwyLzu1u/5CRonCrbcf15LUBv0L0TQxMJQndQc07V0fmU0Mk6GzFL/00Cceded7PnkHgm4QV3/1GmgqqDvT/l6muVwFyHRbetXaAw2IwgLQ1JW1HUNAb+BfEunsnF+99zpqsf7U/YtAddl3cR6TaRegrPm6z/5saD9YYDpaa9GEMp7XEh3dwo1jKAEOY7dhwIDV3M3EFWLQ1AlTjigxoaJVT77iZIiTwAREyjZ8DTaSmQmawJVffvtKWrE7oKx959MxTiMxuW5ztuU6DMIKIIANdu7CiV2RHlUsH/ShpE5m24+vI0uFv/dXFrshNL9KxrjoeyZAmhqpQEJtoXzx8WWYgMzNy/LUlc2yBslgJHbRbSonpFACBE2NAFrDke5tgwlZi2PPXgFhQs4QXEBKyN0BERa84u6QujKQdPcXac0wptJwNyCAlCZY9abrYiPg3gj5R2aXQQLbsPZaQipTuObOL0Jb9v0ihJaNIaRnn9Z8UVvNLx34iLm4SRCU5eCTMRONLpfRZRvKAtiAzTIEmC32TyQk+o9ZXr8eNBVGZqheMhHaJaA1ePajKDWRzzrkXz1i0ffliZByjl8Omh6SBAYDDgjbnYBxzKEQZ6GzI3cnRI9mPPGxzQAgQVpWEJNT2HyZPlSLkH5IlrQCkl5Iu1vKb3wTR5xx0iP2h1IBmlGaiLllWV52gQIHz0uIzqPYgqCN3bgrpdmvefrRfWnDp/vQFBASi4ak7lwhAKPCS884nBSmjVTb+bpfxNSFqUzmjJUk9ZwkkulWpj0q2QgQ69EJIHftnBvvA9Qb0vB7Au1CodznJJAMVzeRtuz7eRZaQkEVMb3o4a1IHThf+W1fZtkzTn/Ikubev95aIiYay7F9TWcR0ooFqKs6ENkV3UqBg350AmUi97f7psC4dlhuqm3ADil1Aiu2wln/gTVt8qIcZHLNsqf22T2l4ATE4X0OXLT/AflgLc9DWL997R2r1o0DMQGC9eWEIndd5tx9VVMWPRo45rAyiHY5PBqDYJSQAIvgr5IlSgFS9uoA4PZkl35XRSYXsGzZ6J71hImZ1fvFblA5/+F0nVgGyEUBO//30eNZgDTuFxCn4KiDKdUBDDHsIHMnG0LywqfsB5oWyhkUyd0JDBDKviV99KLYFiih77CjD3vU4vnzmeDmdTfddO0/mmQS0B+Yhj5WMZtT7Bv9waWEMtGrkVcMFKJdIZ15cBkB7tgZQcJe9l3dUAFuCFl60fGtAGRT549lZQmSExAx3SvF5u37JXUS1mOwuhEJiTLs2o47QLAuaGa0N8ofESYpwpITsJA6YNf/+6BvjimZdreh2OSI4yk1DZRzaD+mO9FuQC3mtEIPgAQVsPBhTz7h0CGAwsaWcFsIEfyvr1w4ViF1CqXZiFsI2M418rULtoNxD32CjqFY9FEjiayQMfUKfIBYUi2GcsFb6fJ53XuJkqmUMJ7IvnFJSO4ilI86osRdKA0sRZQJow26VKo/YlYKFU3fPIQmQQolDx2yALoI4euDH7/3kvsAtdGGMQed1o96ThmnbcBdOXWoFPx5HE+ZLhX0nf6MRx0CZemABBgqhJ1MzLn2axFBRrdP1DAMJCELbP7Bd64nJnpZ/KJT1nrma1IWEowJ0Tm1n/eFgk4a0vsOKUJFIP5oS1YytbFYe4hFF+Xs5yG6FVk/EGSP6iZu33ewkVHZ8ur5kxLM8pV9SUzA/7j9VzFn9Q3krkCSIIRWOO4IUuitGHjaTkt0lTl2Q6rd9qUeIBYc/PRnnSCaz1LpI0OT3MenSgJ93TgHUpkQMiBLGbD31y9cCVmktwP3dKLMn/+DWPK738IoHbvEgY62xc/rAkkCh/TGY4ugKZHgKSQ6W5w1y6ErSA3aQwC5UxLXo4xO9ywkTEwF3/+ND8FtaktmO5SQtXjj+4IrKpWVOP0MrB6SUjh6dhJU2GDYGXA3Kg8ZIkyVKFa88+kLaKYgQQADkUNT+tVvLUsNdeHsG1tPXzmnRtdp7T3X3XAskEXR85u6SPFfzxo97K2/egy9ikMe6ahOOOWXX5aVtAsoa6/IzFQaQrH82Q6AQUB5+OFJdKkgZmAgIFGFXGEu2R4THRvp4jxIExD0/+sPIGADbc6yuYj2QHniFuROu1T2n74vqEckWPGQUAqQQLINfzjot+TJNiADKjxFkvXCD+xXthRpF8AIwWAkaACTGh/63hIvxFUp+/2rm/nyBUsO68+WHqD6WGNkxy2bbt9iCFkU03Af7CocN+1dkX/IUHBsVqqTKSlvwrgNgZ+8MGlKBPJTFxeio8uhVzDxVgPTPku0hTbBipbosuG3kYnuQ8nDjuMnw25TvXojCQOCWKy/iix1BaHg2CVIPSCBOf4hlHQpEKs2PHXwkk3YYBDObi3NlEpkn3mVG1EgLMhVC4Md0vO/vWS/Dg57393sa65m4lGBaZoDBluQMsrSkTneJV0ap4taWeoUin2fi5hSo2L2qwwSqohPPS6F7uzQ2NWFn2Yf9c88IZUsnh1oV1sqdh1DoEspmkXPexgsqW2BuONGMJ0z1l81ouSu2uefUlPQVNmEkuXLkECdlBhpqPmV8T9cjcpkBCl+hjA1KH7qVfUQQFRKHyftBkujj/j6qQ4YSOHP/4pNRWQggRDYLpm2vyoD2CDASWK++GDhLkjhphuj6Sh4+XCaIoh+xeGtIDqm2vMtJpribFSh4P+6YCvY+X6DCZCEwKW/QaCzEIknPyqWyFSGrfchupUjV67DPlCa+4NcU9Y++6jQkug6sIRglHH3Ha2sTAGnoX/eI6ZW5crnFQq0CSPvNEDeeOwBZaBd6UKCnADTnpjmZsNYBJBoNxNcOMeiW4fdt9JtaK58ugNTK8rsiSA6Ott1WIqaSGxdgSuAVTzkzZ+9u7bg1DNJQaLdOGXbjlTsAhJHPPxAysDUdqSuMAGwuOtmMg9Cyy8lTpXMocfhALhTiuVqsFzgwBUbNFgqhH9+jinj8DnNiADElBtQcEZlyjZcUcuKhBMzpHXfXTGBqJ7IDjPhq5E6CT0VNEWO5QkPLSKdU/5Nw0QM/6JbtZ7+lj4BFl0ankGgo4H83CfTkgAcxOLOZCN1h0Wxe/z1jCClX6EpCiWPOxoLEB1NYy8IYbD45wVXHxLXffdqQjlF5pQAFaLrFJ5gyQpSBRStWn851kCaIYijX8emS3eKGNmN1IUxt+8J7iKkU44sA1Ntnthf0tnZrq+kMCHMdSOZKyG0hvYnNTHdlrUvEdWFg4ZnFw60p3HONTemYCZuknb9AQ+cVi9DUxESSw8cLAWIjhb3bkGmo8W6j7YkCAVTLMZAAmQYEZTzBMMuULnPmy5aHZcfsul/00xB4pEA7oCrysD1WxHdKpSH/nC2RUeVi49j6lXOPZvQwaTsO5BwKvT2WYUAgYEAom7EihCmkVe7HiMgRFXSzosuR5AcIVncdx1OXZU7jiBMQQb//jvPAtGluPEqkqQBgoEoevDqFADkIAAeBcFNgFx71T/Pn/PKA+Ln3xw0Q5DyHBnUVUp9zVW7SHSX5pw/vxnoKHhWhqYscMC+SR1wPvp1FueK6XHPaIUqQi2qDbBwvpnX/p4UQLS3JdG6chwnczUEbrgOJXcq0+WDaNJCxpF//x2RsTPu3suETQiiJ1dvjwbSBSBcDshQJK2NB9TV/+EUpJkB6lvIkkMnJ/dxxxuJpvvgk09tRbrwQ/crxVRLPGSw6CJl5xO4oZS/tK/UBKTrWUPWSBy8vgwgOjjV+OsussTkB2hsa9aMKtzyB4lMtsQT1vGj4CQxvqaQJ9Sz5r4NbXKgQC6AoOA2QlZGJzn1H24xQ0ps3x76WiDZdh7Y8LlvbRaoO3OWTWcVy45F0lQBh2FUYfI138E3QD5kJUJtkG2YRNmPGAdOrrcEVgUpsvqvN2AzpU7yzfcRHAKkYuCeQxQmS6H29iJlMXUJa+8kmGkTR+6m3SqUiXJVkN2AACOT4om7rBkCxN/+d+s+dNx+xZ9+uZrcmK4t85MEbgcRHoNg6sw+gCtI2aeJxR3kkCPRMQOnGjdvQRMQ824oS8ACUszKP15MJjHVyvjXDSlQmozr/4PIZAc+5kYDyKTG9ZsQ09dKl7aFMtRyX3obGRlczDpwV5gxDNe944xT9z9w/vrtG//x+1WQJTNhFVttA9gGBhJmqoW4zzaVqf9fV8UWdwU0NxNK2xiGZYwbb9yJmWjkXS5ygUmKjd9cvAElegC471vP/o8V0Wu+9SUyTVbksWm8WXC44c+3M73NtWUEkoGAgHdqCTDVBg7bmDRTYAeAuXO2jQIKTkxi9K0qbAMJivFEDxi4RmDblNnoh4ieJAxsuiNlLi1ANpta+uL3CZ6YwpJrHtwgmJBz41d+SyxNTzoFWHD47Pptm4hMeuACjxcFQlo2/InoaaYbb42JPiBluK0gE3Z+2FZm0IRwAgWlxOSaX4zXWskpOWd+HkRPJl17W3+rTCkVA7yIrMnk24z8/lZq+UeIASmnvPqjvyCYSQwc+ZVTIlBc86k/EmXsngAHlwAxMeli7qrUKApA9lTjX3tQYnon7bkdgxbvF0BKK4FYnPYoh5kDECGGILmtGN+y1062bzgnKvYI5uRbXHlfhtEUQIjMfc4Pt88T1cXNv7vssr3x4arQd9Y5Szdc/5erWlDSq7IHQWmmYuHq1CwKAYaERu7dgvE0I+gmJh634IWLioI5N3hGaRdTqJDx8O/etu6q371qLjGK3t33leffftdv3ncZMEUoA45eefyB/fuM3Lpzy50370RZkVuWkLdAmellqc1UKPt7WS8KhHHZxz/+RibEdLevsXJS38nEAkIgFGeemMJMM6VSiLDPvoNAHtQzkgHzFgCaOhSySHtW0J6RzH2FlKJIBvcQIMwUZ7zDY80CSKmmVd+9CiVmwMR192UhydlnmhCAsu8Z6H4EpBiAkAWJHlbIAigz2Ey1pAi2FYFkwNeCkJmRQxi+2OW4CDnbP/DDFJKZEbXxVgUgXbwVU2RE2C0ePVDq/qRdQRI9LYAEmB6W6U0BnoEI9L97naG885u/upOQbM8EjlzK6bVw14RAaB79KML9zb+dMjOyYL+XnLTp8qt3kCXEjHkXgtpdvxVEIGXPxf/H60FNDxSpzJwwM2Xi5h1fBCC/hNCLOv/fOmTIycys2WUyttUp4IVAQKKD2CSy2557U7D+/4anDSCkmSXyWRLtRghgbpANEDtBQKd078fPJxcPmEZegNoMWHE1AQQmpQS7ef0X/3d7Cz1QEjh+LDPVQmQacCC7jMNuCIG7f/1QivAAiRi8NaQ2EWklGxCJjONkmsh52Qv2JYUHRIjcACBRR3YDxLB7zXSQUhp4wokk6d85/R/qH2Bwh3kAwgp2AbAJAnIbxBb7P/twQPp3TTOPZhjNIIEbRqJBXZlBojAhIxMAuQKFZjz9aEqB/i1TyOIMoagKEaZAkSSfk5hR714TwbaDbo3EPIygKEVAGQfu6bPA/4ZJzIQhis4ip1uFLItBIQoURXXoJRZsF3iGCLuuJwHistiDVXkgAQQQwvnKnPs/TZPBV5+OphsQWP6aAwgKPP+6Nwd1EJ1F+9BBD3vhuWcM0LshvfJl3/50lgRBMwE3YIPdghZgIwFigimwCmykwUDJwtr9ntA0UOR1vn5IQQpBPaOJiAe9YjDyBf80D1FHbPbtg6iK+Sed++6P/+AX33lhQMNv+uWV6/bao1ccqdADQhL6A8f9tslMGbihDLYNBqkj0FQQ1TZXDbudzJrdSjOHFGLMoiYroElhDqinJBSYdVlx0zxEe9BUKcQYowjqKnDAHX4vvHFk7zPo49MpfYBAe+Chd+8tXfkB8Uq3j4yO+vtMnUKi8hrnxfBwbXjOKRmadnD13TUbR2qj8Tu27srOmAuoYpgLvQASWAxmzJSKgcrJCohJDLxj9UfyMFlqUwgxdBGhBDhxzL8LsW9o2cFHLUWagJBikDoEqjO6D1p8bWvnKRy2vrww54RtvnVJllVEnuz62pv+8v1v3d3cvpLT/vSXn739JU985G/Gb1iKTqSJQG3ukv3nztvnNem4jw0JN9LnwvRT2HEXCQusUqr95lutgbNmA6LrIO+qFnc2wkwBcw95xEvf8yShG4HD/vbZQU8CS9f4zvlRoBgmIIWYxRiY+FC+z8plF5Wte3/9q7/+6461W9edTeygIEimvZPoO+oJr/zoF372z/OG0EA555T+7YAu8IXse2PafSYQ2qTs4S9/3soAH3bzaWjW5y977QrxgtbWRxAmIUYgy9SNau940tx5c/NA0Zp3Mu3+Y0TTjcA1VBfB2vrR23jqM5n4liOJOQK2MTMGHv6LGzbVbX8+TE7tN955MGsUYl94UTOdB0hAUKcVsyxQ2b//kWcd3yk+4+TlB/bPW5R5vGU7uf1rZALFKCpDbXDxw+cSKqQVl48Utp38KuIggn7aumFhPOX8lz72125e+YpXPf9BqC1y/HWr/vzOg8O7nd6EHjZmb3xTdk5j9EnELkQgzM+B2Cmw0gDjDlFlK/vJ3WnzuYdKzIBXI4TUprzkog3HvjVHqJsAyEUBAqirWL+aMDP80baLsfHGiayzyAvL8rvx5UQAf3Dzjc96chY16+nHQ7SQ9tlHPvHlH//xtet3je15AhEIHHLdMMBoGC+2rb/l5vqVb/rQG1+wVIHq4UP2PfKgQ+YNzJo1cvGA1BZ5vO208YbRkfS6rth/7tIX/fPvP/3z1TdtahSj9aZd3/MsYpseucf2r8I77A+Qz33l5bv9F15bjJ/VxQsOfcnX/3jbpZ88e4jQAdXeeOLYyD2Pe3QrI6Oc9aFtQGT6J67fmlMtg3XC4w6hHgnjCGIO5NjG8a61CjOAeOKvv/uxcx9/SVE8jSBNQNr3rnLNgUS63u/RL/38rvrIiPceBR8qN35kXxAgFh524BGHz01jtt0aK/ziUBOS+l97yrbRrRvednD89Xtb6dsP3vgsgCApDT/5QUfOKcYTkOqjvnKQCjTwti/8z3NWfqxo3blYahYvX3/V7Tt3NlbQXmQ7tm/etM1/CRKSOOA9t7W+xrvsV1PL0KNW/WHea9P2EwkgSWbJp9Y23Crs1rVnok4geN6OphutHXvs+86p0ZfNBGHN5SFVgZPnnnTOfqNRMowIKJ+jgJR0KzNB5x9490MxIYauAl+x30amLgLPWLfXro/bjX/uK71wU7Ox6oNzFRR04D/UBCi3b62PtnzxM4ZB7VR+6K3hymdsmrvg/c+2X0d/kCR/8EDai035rGLung8egqii/Rl797bOJlLLsnXJLvauW7O9vmXj7TuaJxx/9KlryutqSECElbdfevgb3TqbPviP76zz9z/g1fsjARKPv6vwyPbRO/++vShb71ZQVehn9ofqRfMTN/tvZ93isvHDR2ZoctTeMwSuw1Uu1TecRuIqYBJ2BSx8S201nk6cIsX20FUIkxBDVsv2X+2b52LKMnUTeMRYcfuCELqJfMCju0bGR3/6rifvH/PIg3+7d9xvJVPk+JHWyM61q++6bf+1jXLbq/rIn/7SWgiSAiUfeVs9XDBywNKF7Fsbbz2fKAm25rvqW29ef/NjX8mca98BokPI+nnU1jF/lqgOi//YXb/to887bjZo3tKD9s+Ad+3xBcS2AE/7005/7N0ePStmh311t8t7X/Yz374I0cbc/ZuNH71xpz+Vn/KTYtzPJKuCE/7cbNZfqlv9V/b99FZ75+9OVJiQJIUwEU2NuaYMNrKzkG7/5Xs++ZldwfShD7WIheBIBVWk2i0LgqZCkeqoDoqgCUkh5Lyq5W+Qiad/9gBCJ8U/ungxubqB2c990lfK4tZZoJwcsuddv/OZZJJ4zL7rRmIgv6gst5/OvNqCNf4sAQg++INPKRQGqBypj+96ClFK+vBFA61+WHleq/zJd8gD1ZIyHrW+5a8P17p4cdaovwHz/99x+x44kPc38/eHE0bG1xypTJI4+jsN+6ZH/bocfSQDN9vbv3j2I25P/xpsM6wNxaZzeYv9NtCnnS6rBQHSwg9uazVWPZND1vt/axmnXrix6V8yISlmAWLWjQKaksTVG2Jhu5/NV3/3j1euu/R3MXUhh4KA1AJuClLbNvb/EKdAMP+Jb/jch845GAIghcjKg9EEJMVabeivrdazY39a8K7XnUlQVUxPL/xXEWN3wJ8Kv4fBvn7O+ftTY41Fh6D2wGoIff4GxdrTOfCSny35hIvnECGWb/hUCjCydfuaNfetuuXNY34dmQSsQnlt+Mvwg1+RiS5yHr6lufs9EYjqHre7/GScf4UBykbZf1v/Adfc9QiyIEUevtXe/umX3DTa2PSfH3vIR+757kOftGPHNv+AAFhXr4lDT6b/H95yrIa0z9rUOokoKeOLbo7+4MAQjttaXKLBAb3vV//zm+dMLEQgzyHEDgFyptRxywWxL0bWn/+9OwmD+nUhOoc9AtlQxiq7oABuAC62HUaYtMDw228ZK+1y49v7yEKIGbx8y1Vz0QRiBjyl0fp7P7B8QVMIqY2N13jk0Yv3hdCN8vi0VuO+g5RnWnKzL1aWQ6hQDrD37P7m1hOJB6/yxYuu8PvbxOK3H7h18x0b67Nm541Zowse89Q+qc1Rdv0tpD//LGQNusg59p5i7J5v/uQrz51NtDmr8CsYuHDnpt0bb1n4iiK+U3H5CrJQ8Yj1t3zkSM61/eM/+H+Hl/fz+E32yGOJIsV7fpQPnEw8ZTT9NuRZje/ZLyGTFDn7qj9u+vupA32HrPFnGdRzvPPBZDGouwAHvO7Cy/7+tbMgClBG+PqflqGupBi6IYUPve++HXf+9KfriNHxVzfkZRdt6KW2yqCVjk2/iThZYvFv3SpH79vuli8/gfahd437ymF1pRBh7ukvvLIoL/5/zzzKp+4b6kJIIow33br5wtV3/+hQgrqIfN/+MFnMOWpN+fugEKI6ozTwynLgTdRyXlD3OQtftg8CZA586sELFy1aNisE+F0GVSRi45hHNcv3kzVVIUk5p24o94zZdnHzU1luuujXf14OfQf21zj57c2hi2oxgywEtbNiGdT6n33+2z7faJ1P+6Ev/tSTCBKJz97CExfV+IL9WvpiTe+031iV14ZO3u2vEnj+p1Zkfbw7NZ/EhAP977u3sO36Lx8CWZYF9v1RGj2aUCUpCkATu/n+h56woSCzyUYuBHewkDC1Op0Mm+XFhEkSi/6RynWfeuLBJ35mW+kdbz9mv+Of+6eyte10Ap1VhFO+dP0ep/GxRlls/sF/auQmBwSIu1cU46N2y3+uKUgxtAWO3lasO5QY8nDInb60RlCXJhSPbrVWLYgxBP3Ur6ejQrj4LCCN7Upr+w7d84iQdQClF8F3yRpKnSKP3tgac+PaX1yyqmiNP51VkfchgTj7JTT+9b3Yl9dqeVRlgCwDeEK9vvsj5779Q+87EyAGydnm9zaOfGkzP3JDa+dK8ljjfW6dQx5CADhmV3HRcY85GaA262tu/uDtn/v+hxYQusCBfyjKHX/61Y2jpXd8YhEw+PTrS39TQZKQQgbUDvzPE5grlJA5QUhLXmExldI6O5TbGn43cZJC+Eo5/qNjaD/pUre8474dZUuNAbpXsfADK6EYrbdSWdpuju18Yx+hPeflHh1z/Ybt42MPIorqoC83/GmyLMKCW33DwlABoFLGJ+yPkAcFLXrsd35pVe53dHHtzzfv3tlYuuus1xfLE10WfSem+sMUJXZBzt7ZKvZc+LgFed/+3xxt3ThHApAgGuz0dOrpJVBgQOx5vYpy0VOXLqfRCm/KgdbnBmu5qTxucO+V4dZfM/c1mDJ+4I/Z505sBcT8M173kn+0Go3RsfKZes7n/njb7sa42x9DrFxa+pc08r1H9sd5j/yTx333ResXnfYQ+i+bbwGCWDDvzGcug8H/niHJBshaZx3Y6kbczAAbbMyBhTCl1+wrTU7kqHGfB3keY0bfB3a5fe+19yF1oeW+50PrmlqrvOtpz339hbdvHxsdK//+3Kwt9l3uvb7uibP/6NZjyGPtrR8YAJH9yXtOoAYHPeaF9/ju+eShA0hBf3LrqcqDFAClYv7smEYPWbxg8aIhjQ8PUWmQyoMz3zJI6BBezzMarfSr0yPQFwd+1Rp9LBGQRALQwMD3Hz5r+G3bNZiPZfMvvNKNJwx+iepyrNmapdee1aQycRSplX4xPvr8o4pASG8emfWjBYWsPa//zOsf5mazae8+9QzbrUajObL2rn99bZGC1Rc829teAyGKoffstQFc/uhNFu0hef8XPesoKJvzwgGiWukJMt3KubFKJecWFIPrzyQwudKs978thEztGRz/vj//4/cf+I+7EN1nH6O16uNzv9H0m4D4sPta9YbtJ5KHnEc1Rv3TefBdN85ggBe7eQqBwAlfODvUOPrn60fLemPXeQdD1iHFK9x8TIVCtqRfMtw6+fO07xknxCoZi2BfKYUgbfu53vPx2fQdfdZDhwOvLv2aCuT8oH0P3H/BAcN99+0J5fMfCgzCfm8OJGB56JKx8es3bNmxZ+vGnTtf9nafABgQuxk+4Ii92fxnAKl2w/d04CcJKD3zO2Mb19QbjcaPn3McJ1/6t3/W641G/b8XLY4KnZYWv+ssYh5CrAVO++btB9Tqu67/zo+QBMjhRe9eQR3F/kbzpKNCWUt0DlILNqWMTeEqQUph4NZjiEylAtUhAgGQUTfrOX1xcc37+XgqLhuuDQzyzlT+9cnf2b7nDLIYB/9cph/n9Mefe/w0hSWripuXEoSAXMfda9uN8brXvmMBiy5wSfLL6GuTpBVzI5tu27ynsW7nHQ9/r+lSZneW1i4OeQf5ZPaEU4NO/+W6vZsvfRHvcfH8iqCDvnLUnAFRWRTjzfE0MprGb4AEAiwYGfjdB0H9c+cvZGdLizHt4o9uvieI02eVEvx1vP60B7Ui8KLFj3/4RR6v1298BP2at+DbabTRaG45l7w/dkABMag9ZDD3wfO3rFtNkA2IIz7zaOohgNleXCIrrghFqgql1I6mAhbtRqSU542v/pTIFIYoOitmQSGLpnt5Cs0Pc/bA2JZTyfM477rxsafCMccTQs5zUmPjAdRivMgjJ8L3Sr+ZKEkxy0K82PVL3/DR0Xo9lenW01wNGZ9w6y8ZWZZJ9IIBZ9f9aGDe/EMXDO3XGHQXJm7YWfr9KM8DKHV2+J1Ohd349J/TrpMqIqdfSmvnlh0j23ds3vW640ZfeWdSCIKwB0JIs9+6cOGS/gWzBnKXFKgi6Td/P/WAyKzhZBc0Rso5jwOwxm++9+LmWKPe9K5X1gZ5a1kvG003iy/OIsYGFKNEdcxoD6Gk3Rx09JKGgoCke7gtvvmPIcqKc73W2zahL+z4zXch0Ltqx+4uDNm1J54xVn8ptayP/24Wv6jVcoghRn6V/CHykPGt1HrBgi83yivmhtAmRY7aVPx+DmfWy9+9566Wv8cXXeTk+lj69nIAqQNyNR9xfqRyL/1VBpw1LvJI/e2z6VZ5Hx/17j++4r9X7W2MlVcMSwDSI/fbee+mPXWAMw8auGcPgEKi7/f4k06kY8PkVDvsed2nj9l1OQTSwAGD4ohoAYZWM0vjrdV3eeRUXr23temuonHVnnFfeVZkMEGFAMl0HJpLI4p2UUwaYcuTv1TWKAsvgB4pdZJwiAHKf37vBohmmsvWRc3v5E0+Si3Lefz2xvqTlMcsCyHjhG3Ne/ZXFnI+472b7i6K+x5MpuqMx4yVP2bowpbfxgGfvPEJvDoCb/N4ed+FX33D8ARKiK09O8dG1mwafiGhqrKM3/u9R33dl9+/kNWEGC8vG4+BR24eGfU5ZBJIpj3GGGjWiLN354VxYtjIs5HmyMaRTTvYtO2051J0IMXrzlyxZyNGOn1YOE+0mzmOI38b81eP+OlvDvuvZr3+moucXvPfo3br9/vqHQQYBJiRPClSbd03eYrbX/34n7fyGs/zYLLiYBibEAJrCZTbNtx41a2GjCbTPfz8jBWDjH2oyPMsnrO5VZxDHiprfNh+HVkImR683bbvPpk8dAhavrbY+5tL9xYbV+aBQWSoyDvH3P4o4kjUvvPNsd3jjR1+Rk69whWOu/b9hdvfP8n4rsfu/fzn/7mj6U+HLLQhojAuKVg0l9ZuHJhg8uDF7x6O4wZ4b2R9J5w17iC3NfbydQNb1pUWNiIfpu/7z0s+j6H4apeNN9b+4PTG/OxLttknse4ZGQE3rBVdiismD0klx592/ImH5lQmW7ZBbZaRq4SiqKxvumPH3f+6ecteIE/JTPtnpVPOWXL3T66IUCw+j+1v+HZeYoBYnPa521+5B0MoT33HQTsv+8KavDRtKM/9SgT6jmjF2ESZIPz3ecfuk/3pHbs0mk24+nIRayenU97nkQptgkX2tKcdt0/jVQxDOvTP+9K+6hPfFLVQFiXluZe89vhvON08O5yWx/1xwRVvqi2aUyycPzT79Hr4GeoASgKUTjnyPz+bQIjFe9/3z/f/7gXnh/d/oH/siV8ef/vPwrf/szj3u9aDHnXXLz/xZjlsuUV2J/kDi6YAEZJh1sojD1h4YmvpnFn9lEQm2SPF5k1bb9p57/r1W2mPkMyMGCWA6CTVx/0mYkFnD4+Xoj2UzN/bJCYqg6B01n/v17xiDU6lbKYKkM1lG8FaGVw21sUslUsvW7RzVv/qCgIKyGbO0t0bfWxQOvy1J85vrf3z+ZsDaYwR4Rqn3Y29fhUr4CRp+T/2HXWZ92VU3niJ3BYZGwxt/wm37NCTfzD42i/kZdq/sbm//r73jD7pr7WWmXKh0Pwy3ao48JXEKWgPcgKI5T5z5i/u2x2PPKk1clCrb9EOD4zVhssttR3Fug3rNba6sWWESgWRSMyQBoJScAJU/+JfiQnACJCFKwhOZMlUBhBC2b+gvp3JFIJKE216QSuyusxS0NveMou/Xa8EWNoQXEJg6pAYWNDcarKS3ljIT1lt278dDhya4P95a6bSNBRGx5qrP0usYhcwKChhaPXVH/qLgSdfHlMoqdF85k/uPGNTSCG4eBOE8Me/1kp3kd5yRApTBEgBSiW6zmaNpqzI8nJMprOCwDZgZk7pOhQplgCmOthMbgBBViZlaGLKLjCglZN++cXVd0pAetgT+Oae2NatQCINLAgqTQwpUansArzsJT/828f2UYSMIJiHHeoi1Pdk3rSnvp6QBOZYYahonE6K/0AgLIcnrL02JgC/LWuch+is1tCbTG8KgSQMRsYpYNkES2qzMdVmBg8q1TbloZJk2tWdsrtNdGutew0CkEogmi4VENURItUSxlSHgYAaygJyKFHSbYwlk5wkDQJREk1HlUS7JxR+87+15C54+H4OvYHoVnRtmUrL3D+KdlXJU4Wongh7qFSFG46B0gAKIpmuMgi9LCpVEcM45lnMsyAOAyIErDabZE0WMABBpASwAKKT6YnAf+2NdFa59DjT4zKVAquKGd7OIBDtolqAEW5zVwGQdk2GB0jpU0G7QUyqMBNWBZhJlEKQNKG7nryxALkCEGDAoGDA3Ip8MKYuyv7FqNfuf1UXoDYUQIgkMF3LEBAW6hBxG7hC0oCDbo0ATE8Ly8JM3KidSQygKgvL9KwARGUbYLBAypDkLHBhXlbYOB7Cv6tCIGScmLjkWnI+3GqQx+Dm4EBzpKVQlE5MqpBEpQGDqTRdC1MtzExppru6EEIgG5IrplBI2LiDqC1DNhhH+jP074aEhBOmY54NL1CYO79v2WzV5qhvUcyiB1uptqAcD7XYl0b7+oud9ay/PtrYE/N6c1MZ9u7ZXu7d3RobbRZjzcJmokECmYQBtwnAMogZZHoLJFRhJ7qtzaoNzcnj0GB//+y+4VryQCzDSKO+d7y+a2Rs++YW7SE4uSJwUIYMOGXjGwrr3whJmER1Nn/B7KO8YF8W7Tc4Z24Itb4oJphSCGBQmRTFBA2teioa48XoaKsY37272L5xb6vYsSd6U6NRp2tJICpoNxhwmwBXyL0nwFOiKneS1CbAJNN1HJyzIgwuWuwD5g/0z50/ONgflQcmnorUGF23s3HfHbfdsR6IoerAgFFyPxtvIMC/A1IQTlQOLJq/bL+hFQfP2m9+bZB2K5V2MtUCGxmCrDYwWLIxSEmAhKQgIp1LqzWuctf4+Ia923aM7N67e8/u3eNjzcRkShIW7RWm3chVlkGWK9Tm7gQG1JWRQR0ECES7bct0X2NosH/egrkL5s0dGt53wfyFMWR0TrYNBre5g4IUBdDadP3Vl/1zjBBQ5MAi2urT+gtuol26n1OWRdq1YNkxhy1fvGjZnEEqSxfGlhA2IAOio4IsLIxNu+02ZCEBFh0FRkgBIl02x0fHd41vGy3Gt947vs/eLRuLPNR37Wm1VJRMbsAIAaK6QmAq3UGmS4EA09nGmIlKA/3DQwP9c5fMHsgWzJ27xLMHarMGMjqWxrhCCCyqDWBEtVObskh504/3h0yBOf1DIbLqB7/dBAi4nxNA35H7Hr/igEMWzwFIZWnjIIxBKEYhsKh2kSJQD5mLZllkMYtJOQoRsOi2TMg2gAwSgMFtEigEuk2NltQY2dVMjOzcNua+PWsarZSt20aL7Y3hMuyu07OmWzOp6svzefMKzetbuLxvcFbf7CEtGBoeqOV9oqOdnLDaJCSqTbuNMK5ql0AYAdgONTZ8/zBCjLr8zE03XnQD9IcQshjD/Ryzn/eOb1y9Y1YGqVkmSQoKUXRO8vje1NjVUL5h/a6Se0fUqhemLHa5L5XNohwKsc+1Wa3YNzx7KKM/nzOQLe8XQyHN7Q8pm0WXKdkGBAJhG2xAAhFkFMREW+MU7GjMapQ7txXDcbdH0tCu0QatDeXA1rJ/ZE8aGt67LcVaMZrKwHij383YN+qaa1k9H85KDS2cU3fK5oVGbUk+J82ePSAN1gZmDxP7At2mhJ3AhgBCSHRrlNqkGJjsMtlGEnJy4XWvr5HpLVdEyAbHm0XRKorSMer+K3DmzYXtZqmY0TE1GnvGmhv3jnruxrWbdmZes7koRlKMoyU9G/qAIZWzh5X6ly0YHJq9zwGz8+H5g/1RdLRTaZCBAAJkAIOMK4RAwRCxCUw8lTiRxXKMkLlOCFgOcgoxIbLUSHYzj0xqSiKRKtSGBQJsJCMsMBikKNEx7W01x/YWzS2htn1tmQ0Mh1p91vBQNmf+nJgLIBVGNMYKf3dOjM1/0RdGmkue9M6PfPzNL3vS8tKE+61YfvCdpihcNvbuGk9btoxsa4yv3bVn51hrLxOVrHWQgYABC5M8QDjM48DsWfOHFy4cXD6U7bN4bl+tv0a3JbYrAMlCMkYYjAFjUIXBCMBWABAgAUKWSEHYKBlINsKArApZIASig8Gmwk6iUiFEql0vttX3bt2+bltzz9YNo/Xxell8MI6K/XMWDMzb54hDlyxZNARujtXHR/wNYsxrkUecf8/+gfZN339czv22PPeRw6M7xsvmyO7RlhNdS1gWgQCkcou8XUDAzU50PzB7oH/u4v0G8vlzD52T51k+K8troksn2yTASAILg7owArBBCAwOtJmOAoMATKXoaBDtMhiQwQmMRAiSVEGrsX5n2rBr231svq+1YbxR0v0iSgqbLjVn2WFnHXbQfi7H9u7d+wiyGAa/mpyaBivkl9RvePb91oQVKowBM9MKEGoziW5jf8zzbPbg0Lz9ZrVKzT5s9sjiRUPDA3lOZydMabBQm4wD6goDAhBWlcEVBndyhRFGtAckBTomQnNkdG8o7ly/bVRr1o/eO5YKupTUZtwWxmoTAifal5/4vXX2Xp9HLda+6+Z4QzIgNw790bfvxxQqjAFzPyohJDApMfG++bXBeXHOkjlDxUA6bMXA8FDmMBRzOic7AbYxEqqwBDIIWW4DjIwBBAhl2JEui9FGGTW2Z6R1b8h2rL6XMLJmbMyt3XQUQQhjm3Yz9ULCCfZ96S9uv+kMcr3fY81Wi2qL0w+6H7sfF10KEOqUUDJdD9UGZ9WKbFG2aOmc/oHhOfvMyWv96utXDExDU25sZuXo9ma+acecTTe2BjetaxUwOlp3k+6jABkbTPe9UKkQIrB0PqJ2tUfrzQIDTiYs5P/o6tC9QSAQQhUuTfdZ32DWP8icWRpcsrRWb/UPzM8H5uwtshHyRaFZy4vBUA+1vsbm8cG52d6RopanPcVdO/vGtmQDrd1l/c5GKEcbDk0mKKIRBhmMmUxX9K5QzAKIwOl/s1PLRsrg3oL4f7TJFxMUoDa12TaTKdMelEIo81BAVjYhhjIx+UFWBwOm3XSWJ2NaKggQtXMu2WqA1pbrz7+V/88pC8AyHQUC1JWtikT3wiAZhLqxq8z9agAOf/6RB69fe+/V62gHAFZQOCAURAAAEL8AnQEq9AGYAD5tLJJFpCKhl0vOXEAGxLY3cd6b8zmgDIDHACGIoz+7/0nrPs0eO/uX7nf3z3fLA/lP73+0/ZT2a9S+bPzl/2vvI+Zn+f/8X+I9xn6Q/7PuA/qr/1P8B7mf+D/y/717lP7P/yv+p7Bv5j/eP/V/lPd4/13/U/x/uZ/t/+39gD+hf3z/7e1b/1P/p7hn91/33/69wL+Sf5b/yezt/zf/h/tP3/+iL+tf7n9tv+h8hH88/v3/z/0P/D///yAf9r//+wB/xv/x7mH8A/fvuKf6T+Fv7b/Kvwr/E/kr59+ab4r7t+x/lT7NtaHvj9F+ofe/82dQL8v/qHmLfh9stqX+09AX2D+ufsf5E+qD4h9gD+e/2X0h/0vhT/ev99/4f9n8AH8r/uX/y/vXuqf3P/v/2f5o+1z89/1H/v/0XwBfy3+yf+b/B+1j/1fbv+5X/Y9yj9Wv/B+f7mkopamfS0Pb6eGe9BJIsgPczvOL7dCPlapiYEL6trvnzPjhwvq/o45CkEwISzk5n4rXpNCXrgmezVflaK5tCpXGA94kYjxXQr7xmiaSf36Xb+B+2drXUMm30jW96mSvEj1sHbshz0ukONuzSLrquq/XdGkdMHlb12lC+BF+yl6iZct0Hlnkd8Rz8dRk03Y+L9tc2PCFPnKioeupXucUHZdSWZBXsqMNeXw4/hn8FcwEakoBoD4j1qv+fhS5RtFLxVO7ZriVAMGBpFidesqumkR+GGdaPIl8tQcBgGa0SvlddHhi6rDUus37Jc9KNVQXXWGri+nnZ/bL0YSmLHy25UMxncHZFyMG47acQhYVP20VqhdJaF8yEa8eeWSUxr/a3xH8UOft26+3UFq/ofmZyJKqUW2LRjNgpzlzyWvaf1devPo9HaSLBjsGrLhcQ7sk5PP/srlCvfsmnYa77X111BmsrKYfrDVTs18yBxWaLmS85FZhc9vUbUA9GfVjnkYIEML/SOg4WFX27OFcl6gukdOlVZg3Ub8ooKYvO35LlTgeQb9yGNwGWaO5IF3o+l67WyxMX9thsQHMzUTdhiPA+RM5DCwpljPiNr7SYGKH2vEuilAqcTQRRNM1ur0KOV16Bj2PUggXGr4n365bPsbEM07rlzeg7AdH8x7FbFU1AA+p5W7g9pkN38Y8Vbqst69M4MLPwDqpPkNC9sa5e/850y282T2FutvJG43hFpph+pgRbzDTec5Fvj5FS1G/ur8JTTMoggP6vJGeZOlfT7KSJEco/7JJj4ekDN29raqlO9cDSWG2v2YeiJyMepO4q2kgyrK5oGY/G9a1s1+I+9/GNGq3jmlcb5x02kwI5Op1M/8N+c01ZjFvXG2NpSg/IsUze9wPhUZ+O/A++nWDHQyy2zfLw9vYjl08VIw5FZIFH2ln0Pt+/1Ijg6UNq9+tMST2dF+L21TNeg6PZ62kaDAJZe62Fyc8wlQqAL7pDLznTMbG/rMi+gwX9zVHbfOJ8wEKUPZfBsPFKqFhJIwIvSx9fxyqrxd8R7XYp57oqyzxO7kX5Zhq8i/citQK91aw3fHfKNaCwVsZPLuPHLfDg5NTNbB3+xBmsOeBZgN0olQlAikZJh9RtYSfdkuUVYNmx0oMlDEn7yWCmUd2XIQvdet09d10wGn5Y2PSFTFQYLDvhsBMCrBLk0zpmbXCVQqk4Rf+4k6MGoxAsw/xv74fh6sBCWp+9j4ToA/sH3iUhQVDEVsuxSqxohc58NFTJJtcdI4f6eiXRKo2AEs+rFWu2tCH+5cn31HfuT8OeNpfRMpTbzR8MqK7fHo5IycAKpGR/R3YI/QJ93piH2dfHxSP4najjI+7VuGqbRfNErXpFXTe25rxXAtQR7uf4UnhiUwSv/WFMDuHucfytovc/XDaIH6Ls6h8QeXT3jr6mYKvR/TgURjz9Tg2JzoJLjIUgmAvC2KE8ZFzyP7sdwRr0VrhttK3dMxKurpqL0RXdFE8g2CUJCg05qC8/jLRE/oDRPrxykGiHQ+lMrubACox7kXjQJh6/g6pv6yFkeH24+Ddsm6IYFv16AD+7EH4/mIHd5Ff27ccJ8SPbO8tKlUoW4bt8aUTqWjEp6m8ZOSziGxXr8XzTGLPm5w9W2B56+Htj2zLNXwRbbzssO1PCWulQlmCUsPw0nKq2LX/EfGe2lbhqfx0K4ojQZfZ5fTSktVlicJb/ixKi9YphmeYwyODnMTCNmo9M2I6Q5ir+YMllwCM5HVMHD2sQKsAJJNEh2siXQHfXXsFwgUglpKVdWR370f3L8TTkInCoq2JDVSeEYH7o5RzBMD22jffwh8bb626pTBdMCWv4872KKWdOm3A0sdusPzI+5GrnMRTSJsvKw9MKaWmpXT/9Sm0ZHYi5XjaoD69VrwG0r3mf9Vo6hZwQADvqM95V3HekPKkNzl9FpdbZhMXvlnVUY2R9F22HAAAG+Stba3XkfcHwlLhJnUiv8lZiEKw6CxwqZ5xhS5FhQAWwaFauG75FBKAKrUMVPSxXOJhgrsTeT0vM3J1jWHBNQGn1fMkdEqvq0KSlvwX/wCBRFZReDc76yGEaURhejRvnbmoIYP9Bui5lhjGEH2vh6xwLcOCcebN1LkSK+z6w646PCgAauOtOBvkVjq1uDZPi7KPwDLI5h+OEO1h1+fSgCO8N2f5U9CpCWE4VfrnIn2akHNUQAS7uf3lQj5Lqx7MasqcakeqVsrODoHj5AZwgPBC+UaCrAEZ5ryZrKw03ADUo//VmczwXnqe1Jqakh53NUnSJlT72tv1CHwwo7x4nGbi/AHnmKSsGETuAeK9G10SVHtJS0/6LLClUsVGlzlyh4Pguk3ZTF/fdNEMzou+VLDUoTTaLnfn8HrZXQmu8H086Q80IYmhYK55IT3pMxknFb/RUYnKOnS4+K98X7HoN48mtsN/xzwaQfeJXUMzMN9V0IPFtZEulHAqT4rU6+LhIRXCeNX9fuZg92QmBsVYrGaaKXbYK9tGw43/05O2nzdgzAeGsXFT3h/3SVtreqK+Q9O4fXK3/KnOkktR5wKwMkv9ew+vC0MWlZcI+jdvmh6qJYYza47MisnYdWkE8HQEIdMHBa1yvQ6D2zR2qLkRuWuxPhxdGQZDXvwaoMZXc/4coRDyoJDmnur7kSc054KNo9fcY0waEi0IocBnyRecUPu1dfdLXQ9YVitVhLA0relvOifyv4ZznebusHU1RvyYX80aE0PVKNGdeOmhpTGjy7WcB0FRKXAFFs07hI6c81nPi/eDPolWhIM0y5XGGI75CkUMWXKNutk3hO/Kk3mZYVgWNj93YFsCewzHKPD/qPgHsqAQQEegWT3TmelAYa1NjAUVFXoLToBETY1j5ElqTVAJybOMz5Hr+x0f4nMkE2Xyo1TMlAAjqp+5cEPHAX4VaeO6aiyl0sQlpt6pOlzjnZY6acuRSsHOw/DuuBUP/k9d3eEVpxsRUp1wH6l114qN1jPnfN89msLE9EYQrQPXx7UnDpdmqsOfNtRBUBh56tTnylN6rvHFdqTaY9JA+XR3MAbKMk/aSC3P9wv/HmYU9ZcYwmgc5qPsylTsHCQEDwYNGN3KZ+bvlCz/u6Aw2EuDXcBAiDubL9d5+FM8Mo/a7d7e+QU2sMBXRh1TnPq1LNlsSkn2t0J9CVbDCX1b+k6RnRHQ+8Ez6VHDtpmWS4NsoGM62G+S5ylnNguxiebdhdVABVgev5gbShhyh6ixlJLvwvJcGvKcIHHGzX1ZNzz4uJ798T39KDYmKowwBVg/tQsyZg9gTjTm71U5Mim74tsZFxEeEpriGjotxeyRmkT2+MXqwygDhoe/BQeSt79ElWWq8H7VzYvDWbUDyzT1rE4ZZGERk/C3hsEdH2y01kDwMNy4iW0KS8zjXKspYrAZAiduPIcGPHfk71yUCf4Dzd8E13KC5W0W3t3WYZdjF+exguAfYTIxOCFMePRN8L7O7IlXNZ+rQljt/UC7frhHm0yB9PIj+wcBbdFBtvCl1IQKesNW4+XYPqCzbLmm8vgjV5xrsUBs+aP+bJ7itJu01oCTHD7DM3jqD58vNJCowIslMt2Uz4d+uk2BlxZZIzRgAWF1DqO54NDO2jaPRyneiHuMTMKP6ThWOQ7uzKhxAMDY9KRzrXofpBLcClc7kUppbaSK0cEtsX9VfpshPxvGRXWH1bUftGNJZ9EYd96Ua63nkysxbcVb8Hmtq/upsTrjYT+Sfk80nDkKjYY3q0qg5YrsnX/ItBpDKdNMaIMODUB3fPwFYL9JA+NeiOqtPaQTnOegPYMlz54f59ia/dOAnBCWOD4aEQVAKKf1j6VK/qReRqUr5mQpvKfTzAZu3jUm85SEAzuo2COZ9nbFTwvHnD7Z2QVUYRjkqjQxR5WNhFml7o2Zjj+c9VJivg/iXmy5zIX2+3J0nrKXsIp5ux7GBAIVB5LoredoqczCYkcadKgs2EFTmRwz0scXEhvRdc48iycu2mqY5DdtGG00rGVh6/PHGechbMgFkRzQAF25GMF8FVnTgSKbgJOsnVXMqG0YH8UzrvRh/AasvMcxaao06vCzfq7wGTgRga45TOrogf/huZcGnidJ/wAw04NZ0mM2XjwqLCfZGEcOHN+MyuEDl9TXEIedg1VDmTFrBwGqJc8TFO5HBEL3NVZBtxk/GTXnjZ7g6U0mKEMQYMexwSTDlUUQNTN1UpMCHfY0PHpHqvhEUNJxdKTH3hzDFXmWhtRGBZg7okk2f+7bGLFpMyrbS//igaY5o9xMwM+RslRS9LtLS/LeFIf+c7mOKdp0w7QPZfhI/qsBktLKHkH6X0n6MMhimjynrxJYiN/aeccbyZEGo2JxHv/nlOtbYJiK9rkk/K0nlXL9j3LKpmaAxZHo+ULvUZx9KExqOo8JPfvBLbEJHgbjeXgY5W+GN45RssHOB4Rd4iaYkytp6AvwgPJf8SENrY6VvfMUnBHbEQmlyEC3zxN+PAOT0hxDtdaARGnltLraXTb/gSzz+NrNS4XcLSGvz8WplG7gv6OHWkf8xwPlrF3ili5SCZcxlbcrXlqa1OEen4wzQAPe4A4KOtnUllo4EFOKGpNNLAC2hqNmoutIgKEZ5NftsAZpnzbOmQKBe1XRDaDlfwZi5ymcnA14P67b69EmOCxYkzJIRdaF0NmwfJN1wj2qNwTZtjXzb2fEzJXfZ0mhzI1NkpBX08Z5D2X/baz3YMUrVKANt2QrLteopOERDBMLO+kBjHBGoSJ+uM9GcMhzWIGDoNgyGZGJND+vTaSX9/lkUMVyNy3lzrw8dCY0R6ak3lyKkUrS9W3T8FlmWh5ys88W2Ic4jDs9s1U27n/fbd4SNxOZG6x5HOQ0dxH7bIHLe52lgXoZam4OR2P6bizSHoDK3v596PkaA2cIl9FBEAitgIxPeJdoV1ukBEmuGOUbSJQijMMWfRmtrV6ZKJ+X3FgBttrb7qSUhBShCi7ZnntvZpYDRAtqPBZAcB4gW1Jz8hEXNz255nj8nF808uSv4PMEPzil3KQvmL26d19fSCRyeyOAzlfE4NfhYhHK/vzW8t7fMvfxcQv6HW4xkX3nqrBrQbQvVVtiCm0Fw1ka0+kfFwghko1SK9qWIpG80/3q+lYlAOKuTaciVVW+FOxkPMpuv7DTj7x/l/gR0D2VJYHf5V9qGwC1vU/XTHHjE8XzO5Txmrud4SgTlQ+CGOgvuYxcIsHpamnG3VY4rxF0a93kmUi2KrV3AsUC+VNPQzUZlwDdswHkt++kTYy32ZuD9D1jyzOdXxgTry5lbXPmWoyG08cieC2TNKU6TjnmfMDFCNcDGtCBAryaVA16jK/rKgEJRWj1ntVcVLvsQZdsq5EzV2stEJquTDgGXt++ANVCsrv6s12eg/kd9DaXAoQSDLXuqtxoT+H1A+qVfecy/ZrWXQ+QpjoUOo3g8V0aw5cxBINQ28NOKiJUfP0nt65+xlWuX+J1z0fdLkhvsWeRaBT/yOn8kOH+WyktTuCll0DxGu/S+CdTrtlz5f7C6PpRa6vzH4rtOR8xcqXfMsUXXG1VKKJxkepdB3mKTPRMuy6EOG1TZ7suk52hIYIlWOgyJHATETn2tTspeXmzHU72dmMOPpQNlrs+YMhRAy+sbjPWRtk49fliuknMVXQ8eHusAO1Gi8tGUDuoVc8cOSqWa/gVu1LVpXfQAEh6o3S0r9M91VAnvK0swKx+uyaGfY1Rs00pgcEmYkT/ih9XJ+hVOPMFswqQsEzBWDgyFURL3FOsnbsEOtHXakgvFt5tPgARe2YyqYC6c5xWuP5zK1BbLwF+8NAreIFM9WnaGIYv9zROws0cC+MqhAWxejohZpIT9RElNyrrW2NTAiXc5YkKvdfNklzPs8ee5Hj/PVxA9z733DXULv6JgVU7asE2oViDRDf54vyPFoxp0+JO6zqnFX+dpvjvYbwNx4C1EgedbiHsTGBLkrK+61i2uyjwSJISPqGXfO156QShPbwza4Cdi2JmzLloeKS/onR1JTB3GFXtJxjBZFZwuLNDTO61KMbV68noWco6PSF7dtT1FqPHwP0yAnYDqLmaHhsQuzFuqgGVtM1LAuqTWawM88HdRfjP1Cc+5aWChRFG9TMkNLPsDpIYthAlfyMrSgoiOu6pPLcEl16G6Uq7RdhR78oXpI35oBf8rlfT49ykDzb4PfNHMlisEOVQ/KQjN3LmuiwBQp+0OsX/HRTtVKzu90iKPfR7eGLX8GA4ewEh2Cbq7GhL2pQ+F8CJgSXb7n4m1QApJrQKguEdxLQEli2CVG7HZ3iVJGCR7oFjM8OfDhjfkod4jQdKwn54IkfMrPTdq6BVXIdwb9+3rXAM8JfHendii/ism5BhJGYIQ3bYJL/XtwifBqG1EMz06SkWak7DpShUtaaLlSuDznF5d7aso2weMiRJNTKU+MW/UIbmMf4rvt9VP/kC2oE8KNZJR0z6U3qFnU5IOk98JSQnhjY28QPq2CeitTcOFfJw0F+7jtaFi+IBBqYvPxr7AwESCGy/wwj8vku/L6zJA0vRpr15hsIogjtol/z/mSNICMRpBE7TgJBK1oSbto8xqiJtu9IsPQUWFKwYs6ZYYnC4V/zFzrMIrwkYJZogMXMJArRunMo0ilnMjPHrAeuFicP4tcbuqkeVuN5iCr2NtfGfLQ3Z8IjgQtGN70CQnCNuZkI08DanKHjCWNXRBttgFpYQYlCuE+Bu6pTGmteh9MR+BAAwgQPG7MviDVwKt28UroPJB+baHuCACDRINeAGlJbkLRHdueibg76l28tPu9LPN/H+Dk2wirYIooWbTlIKuHoJ0g/hk00vlOUoIOEKXgg8ji6EUP256qVz3ZO23LV310jEFnm5nkt6i02jUBmTmd6VE1r+AoYL68TvMYXMm0eoBBu++MM5fI6RDOMP8fRbCp12jgO0miOp1f3luAQtGOTSa2q69DiYbYbPCn4wqwjc2OzxVBJJBTIvHJ3t2YZcCtraKqAvaY1ksGbYUhZK/NlRxxngiq4Aay6SwO+hyWfrWIsziNrr/N3qB7nuZ+SUQaVTkia85mfl2s8Ynper2aQZguz+5gR/XmLDXo8kZFnGQZCVGI2jziaIWl4HCkryHuLTVqx2gX2WIyCgG29W5bGu4avpipZcfbBAq3//oD4Bgvb77IaF2NaE7qiPpxnU38hgCLkMwv27YkXayR6aFAX8U/owUyX5pNJrD9sSGUAhapB4nT5wUZOqi9CDThT1HmpOoljcoN5ASnPBCzWiZFjnF12thWzCrw1OP2Mg9iFAbJJidyblo4Ey4ad9iR2wXhuHYwGFccbzqVIb+87878V1f5vIGZorRmW2/K/RKY11G05d52w6kN0gNuf+0WEagAyycL3pC7KOFz+fl5PGr7313leagxHs3RPksl32yupkxVoT6IwkstjSlSftH22TDxqJoW+CD/24bjmcmBA5Vv70ElQWlSw0ZBIEUsKHR+3fqNZOe17sgkbjr2zoqPB+AFGSip0RvxJzy2mWh53qgfq5Ss9KLfhLrbVxz6HSEJEqvQ084BStY3wVaRbWuM3+ijl4kiQG80XS7j+UvCnk2pLZjwBVR1TZ7d8/B2zghM/NPJ5cfyeF4Mp1n7Fub03Q3ZPTNYJAIQ7viv7L7BV7PKuzwoIUExjuocG7McaYEIwGr0k9WtJrNHnBYB8KVQwE+vIK+4gLd/U1KQjrAiywrxcbIN62ECLfrt4ddFCVM9TGnAf/Tc5SMGSN/pABDXJ+6JRcpKZMjhvwg/NtVkqeATw7TYorGgnb7wBfpvwfI4/3UyuzIXA3d4yoAf15NatnfE8RhFBELhqwh11AYaA7r9ZLL+QO91RvRm+rp+Rr1lBZQf5VcNMLhYitAbuDkqsN+m5wCtd4W+VkZ4mG/gqkTGXF7tmN6bn9HKezsEB2mxcSdMjFhL5Uo2pnTvx65pPdTwmIK7Wv4/Xd9+YzcpxPhLbfXa65R0c1LUtM1WFz0gW8x4c4B4ms5seOqnizj+3EeEKhKAEHHNW7FWGLlxHNOrisnuGzTKOBKJCuavdCL3kqGYfnAZztAEBeT1+FiPcnPa5tWu1bDbkii6YhmCOaFV5lSlxRmcbwNj24f3IqWZqI9dFeWL97Xz74/pTRlH27NtWakwkWoGJ6j2D78zMJT5BRFcuQZJDipTr561XbqtICMaRlWLEYzr9FdjBiFWD+QnyhcnJ6fftGcgbWqtMf/qvgYU/SOfqTc1Rtu1ldoxJ9Bq2zSnkz5/w8DFpP32vH0n6rzRp+nCKDL+eI4QWOIauRiOGNg8yZYuCIlE/MvyywAAuPzpwnrgM7HK9535Qvf8Hq2q52FMtZByQZ6EMXkWjk3pmVX5dLlCFKeTcEUSvetZwCkrvDRSBlSRs4AevqCZu8Wrdy6wIWcGLShx6bHNsdMLB56nBAmrCztz5rYFTurEQGWeEZlIHz4kbr2qN79ywU7DtQGyZz30C0M0O238eYlkSNeC8JZSyD+CX+rcarBlwvCFuTMTeky4qCbJlMz0lXMzyKZ2qceT9dxNob9YjsZzacWygpOEy4piyQ230dU+pu0axUKwnqN3OoDDxOs89Kd4JP28gr3xBlXXU7rFBnlPnIebQis6NBO/nkUobR2SRCfpeD+ZjrznBGqpjNIkHL1vNYL0VG9N8GvW/JcSuSAaJ4HmcDIRytI0hiuqOGmNjqWI5miuYIUdgInIuZZJsmwOaaIWupcvwXRfqXhCHazvv+2S/SiQfYbbNRnzf+OBTT4aEaftgZSFmC7wDgRzxPe/AuEe9UrrpEijfWlfOE6aM8mnskQefz/dEdByvBiQ0JFkoCvjLxnItsmMc+qwGnD+YY8J985PFzpeUPIJcsOU2fu8yUrrvAeGJoKZ7EqZYKPC+WhrbUiGaq2pv0HvhdOUJ9FHYUgCzVmyhFyHLk5xssx5NWp88HvOuH9WR+NI8mCiIL6xzpCk5wDWTTlAgWsuY0HzsIThIDlcGW/0E2OZrPxpm+NVcv91jkSS6xBIo4r4fRTO8yH/i91Xl4sGNvv/4G32PwcP1mbqptELTxiVkHN9RS2oPU3GTdZ1S4PYEQLSPDwRER7LS3N8Vi+9/rDRf22XiCqrdPmEIDZjRBrBjH+HTnCjOawXaUbLddt0VWleP8fZBV6+9hS9r0AfLb7yIfrQhJKZ750hbZ5k8Fonc8JL5uMs60B4VK1cmmEj8+zPUcxNgaWUbeL2ZujfP3edLBIiralrFRacuUxnwrNz4EJoFTkatvTXKaJ0u8RzRR9WwPDfDwnapRXQlGkLs4qq8br5xnZKaCtkMkhoMm939mAajSrMsOKR154tjeHfIvXNX77G89N1j18aXtDcoGZ19MHB6yZCMlQWAZOdIml1UpO/Gb2V4n9RrvIo2ZNhyIzGfesylZ/70I7PcZDLDr0QJg/Npvmk17P3cOteNTobueFeGuNBiW81PyjEVs9MCZX5nghXhElOPX6re09mBTZef83DTGktudokc9dnuLhmGvuJpl699vW/4bEMkJAQi3TCaJlYeZRziZJnl1qpYAeIM4+8aSGTro3QGhevXd/y58SruVmbpmHu6GB2qpbLV4oN7kh17oq6hDORSnXrlEF0cfNCOfBczknwDCQ4/yor5tdk35XyQ6oqXiqL9b6AanRV6K175KkX0iV7wECsc7ECgbgU2NHKav75OTpD5CU9HkYH+SyML30kYWLlLmcsCpv29pFuG+9OcDAJ0nCR1TJSoMGgNEoFIPFkHJA0X6FjYdYlVwIjWeZrAFhg2FVphYTEZZY1Ova96DtbYiVsVY3gyjZ3OP4lXcGiEPzA7GA7qq0ZSA7mfVPvLCcfy0ge7cHt8erb35lU1zDc+i0o91MmwERyWaXC6OInOZVFAX+mkQFHCWw/UgT8XeIPwnpNO0FDR4e0Q/nFmTMpq2nMucf7dbZcUAN/u4SvHYer3ciCM6BWTIQPZ2UfiDkfJJ5lEaoNi3C3MtJSImJHAuYHqfp49CRZ+lInZG0H72ii499ZCYNi/K3nO4BbsXdO5EPAhZcJ6qPkAFx/UpNOyxDsvdLdDfXr2vzAV5iQ9pZS+FnCHULbk+oeOnFi5oCyWqLVLixvnQQL0BWDU4gKIKhkCvEv/jGFyQUhpgAytSmrhUUoeV7BpoQKde70lFm5eijiupFLVkjVmnyAH3L0LzvJ1ACM/URu89J/PyqhmFgTLtHcrPqVt2wnK3FhphTDRV7BT2ZGCg3+pzvdxUT4S8DqOuBIQo78Ijon0ZEcRalcEIik0HgEl1jrKSr6SV0QWo+RB8kz83LR74gQyysfIofxvt38YJomYbOP/+RepZhHVx9ytafxWJe3nv5t9bSyCQJIMb7cKB1uuoS0HV9Vp7fOzt6FsdVzEQXMDhmuWNeVezO1ulDo2iPAspHdJ4d+0XwPmd2aaOodlhTfGLTYvm0QkBjaJThehI7rlIOLVw0kmX/bTVQb0qOi1t1qyqY6R3UbrsTkI97maK0El24OtFuJanCBelFNJUIbPpI3RTEfdhjrZhnNh/soy0knXqs2ODQ4bT3Slmno7L0r7D4WxaiOIm/CIhY91Epn+Uh2Uy1VojCsYIfTZG1rYwLpZ3mUw040Wo4aFUnGzQEuDnicF2lj1REB/x8Gn86+l2eAA5dpPph1DYEYVc6WysMeTvxKnUqY4dAjd9RFJnrxZ7LeN98gW1y9brUiLWvS0urmiAUpZlVmsqZLs1lWJPtWL9H6vpqxtxRaQyVYTVer+53wJ7xRlSIzMOiokrVQHmSP6AdgldL/QnjSBCAEiYiNqQ/zG+t5KWvXD6AowhytpjiuwPED3U0pM7/wQ4FpcKzcjDtmQp89dm6lq7VR8Dktyqdy0a8egENqmanDH2Gwhetd58PHpV+udQhLol3vhTpidO2k23YsUShPb9hsRDkOfyFC7D3ASKToc4sLLFXHoj7cYxFkIGBrnM5dpqrkzg0ENe/czIs0D9sRyDBIhM0x7oehoyc3nHA+bYy1A8kJsKq2eeUjww0xOcSDXPWBCjd2n4SINnkjg6OYZ1oj9Gavz/k1xBoUHKNslycXZvX3F3CL+Ym/mz2YWUfDnn4B1AWrjPVPkraJSg0CnuogfdNDhnBjuJe6VJ+68+9C3xlSD1oQz2WVAmpCtqTxBKtCHP4DxOItFWBLhk7g6FS00SKvFe0ckEYkxUPLxRvSwi9f+lV//9Qd//T6X//TnPJ2AjGrCEqKMR/G65AfwTSDEUyMdYzrOzQEE1ah0J5ldZLMwC97W5Zf/vfOc0J5Ev9ML8vzBjlJGf1v4wUkoBhjXxp2tBCcKGy7N1Plj/RN1M/gtuC9+lF/yegMCLAKam9W76Euu7eG9VtkmzjV473z6C/57ErA/zlel1mOEZybaADiHVHMnphHGcKvTSwM+8vHr54mUm91/7RPXL9MQdtZ69iwximtmH5XIJQ9wZeSJnLObHm32YDn6DTfkng/FsaOAAkrGpYku8NKx4nAPLBGxHLfzLu2IHljA7ASg18daTaqMXa67024tZbjptmF6mnejFkIR+8SmfvUB3F+OgiqbXoKDPQ95ifad5pAX7OsaUKqCnbxT/yogrH/K+sI9B0YksEj7Krt/kvt06J6t2Mq3VAWFJPVHgbSrsnZ/m55O9s9U5fo8YfFcPtenGP3/a0tIdLiss5F2mNI7hkq/d+osDztg7icNn/+wWYsntj2PQGVxme5jnVKuIBAQr1RREF627gPuvxoWSxH8yAhF4JbJDT8l2b3sDY/xgQime45M9gG+rnrFwjTG3HLF+aBjoy/McEX/QCQNC2bo/HwqW/1/5FguvitUDTb31ByBUzUnIZpaK1Afg3nvXi4loGL5o66/fU+vuY1yw6v+mw0PmVvhuJTQaahxbGNE8K1Dra/06gdbcj4AI/dIuv+nFh2cdvrqnE/nsOL4Soha83enH43hMbK0jNmdec3Fo2wBYnamkZL4yAycH64Yt/HPBHQIcrKv3xfycmvUVonO0sBLuhelDKHmpv6mhQp124BBAMQ9NZ5IQBwWBnyswaDx0MUwAsvULNdzxn123ZDVslozpm+ktewf6FVSYW3XolagyYW2V3Kptna7+zLM+NBsrX7lCG70sNGPJdutxbplvkSu8qjo8Z8r00Rlnu6rCwe/7+vS4F+PZ1/8TiCrTItJyqdRVq0K3EaomTnYDWBaYU6b97PxgzZV+MSIHxHdO8xbTiHsHtHjs8HKh5T5bGJo3YkLb6dQA0VDVMKIvuqLmhtYYatw2GzI0D+7wEhLV6kdnSEcQ14wO6F6+HBr1K3pzCtzTbW+lzxwvwPiy1smW3rBLBqDjaa+oYvNKij4RK+HQmCoJm9FJzU9K+/WirLHtlYo/1JOw1FoseJLAh8Yvp2hBHBhtZYrslzUfe2s2EyVep8x3Ojux4EgNcQ2PzJVGuH9x0jYxDqlcUfE+DDT/GlHFE6EtY4vgkkcov46KGEmU3PoU3Y4Wfqj2Kjtz0CSYn6BR8+OnbvGYp55nl4FGT/ALlpr/fqIcBuz7l5h4RsY+NIDoOTpiwp+U8mP4zDJuI88JrwjZdvUDuESi95j803ZJCfO2MTG9DenhrNvsOAxyaurvS7vTSmU1FWS0KOX0bx9CCfGS45EhnPMBsjcTlvDMAs3oKmZ/uYmG2pz8CKM5Ap5xqoQaZHsqgL53TCEDPrteCnD0ADEhfxtICX9c6b+IoQF6gGsp8SYkPtgqxNGVUv6XPOfg+/20fT0SB/OBthi8SlbH8xV4HtQjbb5Skz+cUf8E7E/gCFAwZp0zjcGvlDAW2klv5vsDdOJMxfaFE2S880kkpAQmxmn5bAeBf8T61VNrgY96Oz68ktbORf9tSl6aPzJ7/GLIj9eleIKYlQ54H+4mudzQP9mXaJSM11bKWiGfRNqVeaOA7qEQbJDWyyEubSpemCZRZpBJ2Qy1VphE5I4r0AWXNEsakoKcToVt374kkIxHqiCTY/aT2nq2Ll6wpAxtFLctMsSO4P0oRyYPtbiNT3LbGeZk0gRAMGj1UEnnIKd2Y+ByCBQyN4r2185tZJ/kT831r+XA2zR1t/2RddNUjm3KpJ55Z3ucA+TMvQX720pBDHamxrH/4/GYL4Z4Qs/3ACmPqjc7WCtzZPT7xHahuWmPm5xJSKLs83QWl76ZViuQ/sPMpgzU5XYIer+TlQofCVIOSu4e1TRIlj5Z/zqLqSkrWmSbKE2Jd+OW8fdYNx/TSE1Mm/SRyQ6U1rLME18NBeLJMmFf6sPhVLbvoDlQf5E1rnsL9If4ULk9547jP3sE6d8HR4Zj6rBw2IN83mosxjMiKYz9E7+wLaslRPFQRI5cfHIWXYWqrMUzHzVACOLpBdfs9Nl3hVhj79BAOy5aRFxA8B1T2M7/PUWxVYcW5awtqGrdcXhq8lWa/oo03TFYo43W/txgoSXpUGy6u1pcWFphUb1H8Gp7InP5kIt8Bw26gbUsdT5LSCJpj2wzFIEBdempDo3rFr6p26SflfGB/9QZvntkFUhMvcNEkLT1LPR2O/EQADZ7GUdBkq8lqdbLw+FZ5zXoTxrVNOXNLUd9Zasfn6giLptKmKTrVb0wPHeBi4ea+zQBq0yuI64Q7RsIDci9li0q9gzWqb0Mg2cGswNFqL+UT/6EuKQGHfkKlVt/WVVsY+/JUsQDD497AAjV0GiAcMlgbF5o2TjiHHShXCFiXWIksTyifdU9QtzZGOHuo9uvp8ZKcSOXiItcF/jkvwIz+5CZ3298Kr7RSjEHPK2xhYrj5Pv0P5pu1qqBoEk+QHzmP9Mj/j6e/FGjvCkae349Xk0I23k0CsznnCVInr9rOPtyndg9oMTZ8iUbw13A/MaC2rIBHJKtmtU9FeDQxPp7xpqETKA1GsqGwamWgIImF+i9JLjeOUELy50tqG91RFS+Y4BMiPmNNL4qhXv1z6Na99ljIPPq5tcOWPPKO+wpQmmi5YGCdL80t7HrjwZEj4jvd3eBk741NG+CV4KOuTKqkCK3b7FYwL+oMIPvdKOOz5ajdZFzCziGhNIzjc6UtPnv74s+TWl37i6K6X6Wr9pk7GebZWJWb4tK+L72OWf+YmQGu3a5ltdjUM6WVEOLwmHDPKeEsFazeVEN+/A9Ps762LNHJFL5c/uM5oz35//xGm2DIyKzDfJybK6y/NeG1ZvjZ/Plkxliy5I7KtDACYFKtdtslgM2yff9FaNPdRItMizT3FSuE23Nww3Gddr3IRW/dcMhagYGIw5Q0IdPALw00x5BnMBP2vlO3AH1bu0QXZp3yQKAGSNexHqHzjQ2CEd10eKh4bZjDgcF0yltPOOEPqRdduRYl1mMxT4dZqxwRGaGyif3wc0XTZfsJNt1bpS2IiBbOh8pRT1hCOsJkjNV5o70krBbxktzr11r4DRLJgYsN+CWV2Qf51wOEY5PCi5HNPBxLUEejZRnx/YvLCxXHZU3JmTLMHin9AjiG9Oj4DiqQtYhrIin5mXZ7nZAKwbfq6vbl+ojYoqlkjQS46auRdpY2EjFBWS/tSB8HZv7SF3frv0B7dpinXxiVBshQr9FppDzSTX0I+hDI/4smfVynUBEfffi2NyRjnsz/upWCmdOcF4ckgCu7nb8LdlBg8dCGhGsutih0+yCPpoxnySVp5Lan22wdb+Fh3nGSZeHVBXkcT+l9lM/rzIu0T3yO6plkZMZuDxuZO1K8oqvs37MPJSjr3x7pXeWBTVNUq2VXjbacblxMdcpSjC/3VF0JmSlbT5bE/kmMXqwuJu1PaAOSGYxqA554J0djIJ2KQHEwLiGKuGk83c/vf5IPmiI2TLjiUIGkDSN1zMM9wF3qOIIRKGIHBeAy6suW7oSLJCQcAB6Qsxi3tdRosJtYrm2swVeCjlrHUEcx+X36E0VCl83c+5ExkVEiwm/slsJT0Uxv+IHIeZXlmXpmrT92zdRotozvFlrcYlavjyyr5CKElXKCJgCwHLa8jcpJZODu4fa9pR2Gt0IwafOczt/18DfTr05bxJb5Zk1qPN8LPv1QvPbIm2llQ6Tb/r4Vm69hRi9RuIvXqHaiuu/RRppzuiwwd2P+mGVuMISwWlZNzpJtUYLiILOtg0C+pnELZtsgas9c7OKQuMNSLukuTA9EGgbkhRSKYpYXvVrN00qHYyEMzioZBGRNHu4Lmc8I/tQj8FlWvVx+fpR22E2Wo/JsoJJloHjY0bBnSsWEtPq+HGqFAXZFIK3u7I4fi561fufdFTHjyRJFpkv98asP2pb1WrSt2RmTZsvUCc833qT63/NFk2ZV9YppqQcWHvO7GGipp8yl+ioCJbuMoUeQL1OdoImTB6Tqp1pu79pReM5bGpI3HzyTXHUPEk9Cwf/osFiyiVIMu42VQ4TXcY8wuKS97ip0qbOYZC0qHsXDXmZA6Cr55UxWuy5BH944NCuLyxd7/pOo+OQK05t2shh8BYccLC8/RUWywSI4R6b2UsLKbMt68pCV9ZRGpJU1pti5F4lVYdEKfElstHrlHvkwaxia1tE5/zGp/yu8jtm5HE7QEtosUXhXpMLrarzCekDF/7w2cMboaAQ1L4EYRNxtg5LcOBFaPH6Gx+9ebEwCsNkyxytGk4tGYKRBdoMYA5Csxr1xim7DjhwVniK+/Lmu8/K2PW5T4cO62gxLfTqMnOKQuMdNleLlEPL8+AtQiLZM0p/t5ZBvZdGU+mcADzz9nwY+qWJKxVTuKBy+e5pbOYeiVGfmT/1mkBh34yPFzansxTrEqFmdYLQ8j6nuCaU/cZGjPHfiWeFjAhd07xW6iiWNWWNuCWiENJ+cp5C/XvMYG5eEo8ME9UZM5o9PnC1G3Y4aAcoQptarm78rMCR1AM0+14TZQV6k0YASX13fye22QKfeutV8trqxX9QuGlHTIMQgt0cTgjYsrqVuD+wTiPLg3B/A6NGw95Ce0dX9oppvPBvR88SemKLdE4sirOGb/MsAb5yrZTaGusKKLDI2XAeREdK1hNBwoBx1nuC8k8yFG37PQtbzF+CSB7+yOTYEfoVTWzFKg+Wzpc63vmRnmaIRcSYxPM1viok3RGqCDbOsfoBDQGBQpvWyZVjIYv9+PwU/iFO/0joa7+GyUDfnMMnPDzXXKJLbFz7+QsMGv08Ih4nwhJFsL5yyFv0ffJS9TLTS0IpA+Q81MiAQxsJBBw4NT3Mtj4Ut8tcUFFbS5lQwRaG2jBxg4DaWcgcEKpXzvxP4P4zzZzuG1J6T9Ki56M2i4wp200pkwmMC/b8IT69nnrCtylqjO2VI2zLIoKDsQ4zyvk9WV6yDFvCJzcZHQGMWPrBoLj5vgdqFi+PQZu6RfbQc5FZzxAri8pp9Kla5wdngMicZpSEFkBqMGSmRPCJ3dY/k4tcg04XvbIsXV2JeLBi3XIfrSBxnHRrkq4oVtlpTQNsV2u9LO/rBAvq9yhLy5LK7v6tA/BmJ8aoce22FRnCe1IZp4QyaJgEopUwXc2OXiz3nYBd7M6KcLB1IrAuI/2M5wo8eqKl5/hbSwecBfapBHgngJyShlBf0hdwH9cKS0f/24/IWHl9e/Lcw2iKZOZLzHHDnpAN/J6M1AAZ9FP4myMCKHRHhecHqORWh/WcXzC3o6tfkdqjCtsJTLzb/ko5iwvlsqn/Qw7Dj8XMQfMqfuTYEri15zBYGmn4CqHvJFA1X1Tpe/M2gFd74q6twnMDPmky1RgsUx2c8ZzUxBMqil+uN792J48kByz1VKxmBS8X/4Hwla52FFVJQRvCx7xRuatUCXE/aYIaBI1Zj5qys00+C2jaFXkOcK1sBaIZpMm4UH5yVRyIz5NHVqXm/oNjZceweA7hRf6vbtyO+7J2ZVWpTnZb/hf0aYXmv8aojBBONXs1oRB/NC8A2JsP+bEoq5XAMJZ0chayHg35JHQbEZjqD1ntPre9vyiRW5EKQuUJfNF7GhGEuIL5Sh702Oc/LUk4/ryJtaQm80Yaw4bcX4rQZqIGIFQRwz/DLH1EGPDxDL/thvxncQk/nkLUsFXyXN1eNhccvJ+1NS7Nnnw1ousckveexWrtriYVUtSZwHWZgvF7GMPx9FPUeI9wIgEJm1A/Nzvpb6HQ+yvimNZn7dvpEo5aweFJ7wFWlsczakDinLPMfRjVechJ079n3ZREhjSHmo5XHRgLDyySyE9P5jcE+laC3v4gqJ1AtEAfLetbffbnkF9hJcaxgpA7glj3VDRUB6Du8WsaZ9oHxbcv3vP1iXEoOoBHIyaMY0FC5Vmo6aW35qOqxWqm9ZWrW9fjKQ7phfeQ7YUjocNVIAJjsobogQzlGrXhqjrQodLFI5p/ZOCh49+UK4HdrRnHLl/mTmeNlfzqIB7dvxXvuksyW0CR1oYA3NmYRRDfeezRJ/kXcRKeTqXfIMYxi+rGnJSFXQM7i/RCU3D0ZgZO29sy35ISVXjsGiMBs5VpZAEMPlA1wEJUdBO0P4aqFjAGmnKFYAgTbNHgbPXV5YN0TB2o6n4Ya+svFPpgCakSwSt+jcn+EjXIj9IhEq86/Acht89lmMtPg3NhBc6YsO5+wVWq/R1iZxuM48DsH17f7UPMD7QuNdxe05i+z8WWdW7D4J/pO3o770GrAqqlJzpDBS7q5vRV8lziL+/EL6Hm3rlo4MkHqWkR0uEXaGzkeNlMfeK4zqFGryTUXccH2SG1Nm7qmhC/PWcylwf0S65tRoK5WQDsCsCSOb7a5odBg8A3XS9z1AQ+V/phbwsCBOFmiEFa50HmYjnYciZ1nI8Rnzbrz44OAUKnIFFR/nBAWNwqfhAORhzIafPP/PHPBwNbkKgGxHHFZnDeCac4Dd8wkRXViiJbM5mku+11TnGLOocul6FvgmcHGbNyO8ZIkIF8mqO5C0EqJPgiLUYOItuayfHinCkehoHcdzimlCwuKNGVnuDRm1LsnB5HN9GqSgKQF0VRJlBs6hHmmITE5ycH4eDTFCyk4tj3cWEC3dWZD1wQ0n39smlC3MguGFjMYxeHjuMnM1KvUmnx+nobqF6V3nVMXM4UjxGkIdaHVS61VnnfJShrCDXS0jbb6wGKp1I0SwzQQfmfzwKSTPJn6We4N6A18jB9Pz9IFoh4EbFv3SHMNfYTrjG/T5Q96lc9nPzMWpHB3sqwZm66S2qtBsk+fScaOcADYJvWjF5pNckbRtuBHBGjETlovOGatUmjDtCe59WXV/GGe09+M+rQ3GSJ37gU2J93ipzwsoz6IHhLZzoq8NNN14V5Q+XbUw74CeCTegiZ8ShuiO7lfutj0Hu98O/qDvIkqAiYyUsuQARhRRWWp5ai9FlPXV4+jJll272lVqZzqWRNCx6j1XUFDxeLqE02aij6BZ/b5SjyjWFC1swDD9jcqAeA+Ipz18+UcNJ89j/Dr4+oLAI0c2NjtqNTmyPd8P/mphyjkrHKLB3cgSmaAG6CGWoW1Y72XeCQNmIlNnXepAI83yKYFX9MJsutd8NTZ1Jh+HKWQZHEUrq8B61tfQLeiuOkWyg9wY1YRMRLGzGpvVoruI6wX+oV4rc2Y7rdB7zLKIoCyamFZpTbzphTWqjMsn+Ofwxdy6TG3h2TD2irdN8oVKWZOO8yX/143vFA/lV0W+jJKU6g1XvMVc+Ul6z9QVn0UTg5NaL11K/Z2LDREsi4nkZkP+kahtuJdkxy26FL6HKmUymh8IpFT3uEkkDVTE1M0H7IXYKbZC/srX5jxW9gRBDGCVWs+S2IrK/MzGjK0qaAABxNa8PJbMG2u59WWanDNhuKCDRhr1W8AIGunQ1SETcN/iu3Yz1wmeI62C67713z7vEXBqPX2gfgzZjkRS5QgoNLMEs7GHjXaNRquGTacImvMb0nrT9OfduoqfRP+USHH7h0RULHidNLY1lmQ+bVIoZuNYbEyKby9mUOJ6FbSZxDXEnkSL29PP81NmbmE5mR10g/MGsDEPCWcGTK6fiX8GCs/HWtBUUg8nFBBLJzJKzSedF/Z9GxbxihMq9Gkp3iaSQkARgi3tX2mulTNyiiRtoRiK7KgLX29XX02wQ028BdALdd8mQiCg2a/pWu2ijZAf5S4NjQ7tXIDQpVJFRPMjmDK3eBOnRqDTVaQNg3FbMmukD2ypkdz30yPa+DjykqU2kKXWcEqy4I+J1yZAzGeJ0Z0neYgWFcgkTa6rgTdCNvdgaNoYGJiJpg95OQEssBR5uLg2EDix6eccDvT1vGu6L40/Qh2CNOJ7rknfh1UYE7QW0cwNK5Kz9u3CLG5eKarx5lCA+StAp1lZaDS/CmiKnWV7NM0zhukNSL75MpdCWtUV9mFVAEaI7uQYvaoLbA7x6Dyj67jgmfPlI1Wo//jqk0ReGLWRNDtcxcZmKLwRIOdgBoMRQqGxxOk6DjWlevMoU3GZ3/2JCE6lo+YmpQDt2eEOlfIxHmn3ZxpyMYsxTIDg5Zyaf5zVrg1omf3wkMIvIGS6MGLNThsJFsiL0nstu6d1fYE/ASrjMm5tsOop3OYImhQo7/TtpOS+0spYoj4at9aDPtv3efBV6uFqBXis6rDiOi0YD56LfSDZmp7U5yRPGam+q5AgdoSrGjMKZB6f2ibJkiL81MiZ5VSN7mbGr4JKvVjye8/ZCL7yKVK59OQfTQGMSLqJXCFYRq49Zpl4SfFZksaQmWgZxcgPa9OHNUIuzgRD1nerMX5y+St9dgWRTOkcFmIZVF8L00nxe8RoTXaL7Xlwcfw9vrLIwI+laCppMLXOJuZ/c0UwtP/B1R4VR5yaRve/lrfl/rzukZL5uv/dmEu1TwfaQ1uq+6ZBkG8kkTwicaOy1zyAqVtO25Fq/3zRwlkdgGN+xl2ExPqgBhp/0cFHEvZygq/SEXi+Potw3sD0Q0ApcUSmfVyfMwpyUbt6PtKbdUJvnG0msomaSnUDvcVA4FLwdu5ElXO10Ta00q+wKe4Cq1JgBW1fPKXcK6WxVCzDPUxcpYl1X/FhVkvfGvH1RDcNnYdyZr6B5+dPvP+NoPUyzOZonJl0tExqEC9PWR16yewr4eqAkdMtCFwB5DB1sPnpPPxX6sfBzaozWmmR1zovLkKgAtUx8GZWXFgafSN+NcvetZlhyukZUuKDsePD5W1MpyjsEDUtyIeCAmTwNKyNxTA5pf4wdaW0DspEvPmiADkIevLON7RBm+t/BnhNkWQ+KSDBSBAFxqeOEb4jhso3IMiqe1cfcoNKzxCVL2pHhfrJ3+sDhQaM8LC2Ov37wZ3am9+/Cn1APW8kwBDZTa+SzQ0cR4uZVkUDvrkXRhmDDBODDYfAGbgNjP2Aw9qUSz6s3BCFNRj0eBggRMxlL8NXGRxk4tELQaFWoEGX8GOE2Ih4tKMLfHZlxKvEcq9yYxqwWkOlW3PBir8b3/zZZliAd1+iW0dkuI9laC0dCrsh7oC4HtqlahUPVX8zHvtqu3vkUPI74GgNZhlzGl5VUKgEQiblPEaKirYVX1w1bmP6/vD18HKhtlPFroz67JmfSxOQ0czZWqP4L+QmoMWTh5tmu56+TmllZGegPpQHNCat6dsFnvKY+CH/9nZUT3a5DsbfQyXnQ3XUkOW9Lmg4kPsW7Tmt8EtyrzuwVq4oBuq+CtL7iItYaLPVwIKFxZ1pGsfQqDJqgviMiWynOD7oDmce5fQJBW3bvTL+WWU0FZa+LLToJNQsOIb01ZvLw+3kJRez1BsfFMVUNUEaa+v7xNZhUSkXq71zLLIaD7yLiFFLxWdHPaodxwuA5ZAX6DOABKgla/VGMHLxDiqdwyiQprkmDxciob73fh1kO45J2w1Rspm+/+BfEWiQ5Llm4Br+Iv+hvv3tiYIm8vPNtTKPMWEVqSOhABZzW5F01/Fj+IGR+CwPS+vLNsETP7SNTWRV3mbHsW8vgIvL5Dl8R2wNEGf0anZTNnaKBjGKP00TIP4zMSlNMMG4ckfaAgRw4WEM9UDbSNX/pwEJpX6e/mF+ej2v8DWWJbNbxyvL0drfFTsDvqFjmaaQChB2cztdYTafx7pfEv48uxNhcbaqI5EfCP4oqvZpUMdb0df1W/M6XrAUiZOn8To3uxnCxdgyvg9w3dWTZgTfmgqUhE0GiP8IAAAAAJBILViMnpg8KnfgDQdSlsAzmSBpCB0F75KliUgabCkKI+0vHWbsGG114U6cc1cjLHoeS1eHLalLJjJ4ljdFgZgBjgXzICyiLQYfcp3QX/ju+uNc3yoFNONJriAwUk4UrQifKpsnYGb1OaxsJ8xXty4dIN+xFQlFChZoZYDUgltUcEfpg7WfuvNY5+2ksefcYjlHMso/chkr2lgBkOKoVsERbd4qhr7kQ7ZDz3eaoXtx3Js+dpslDvQH6XOjSkElPpQNHg6dECGHkp/aKQXXhfwsZ6TP9YDD9oeb2i2cWnBS5Wl5PA5ZCjIDis/JeDH/tvmpCRB6fueP7aIPmxyave0j8jOfSS8E9J+nxAqZoYVhbRWzBb9EJPKGXWB/ZV1/KU2JnH7O8Pr7L4rOSP+AlFynDz6IegJdA3yc15gIM1/oPWpsf/9a4eroYv3xAHmKcWfwAn5fW6KATwnxp1KivQHjnd38DBR+cNmSiF7lL7Inab4DigXs9YJymlvljzEO3nQu/U8oi8Ypbe5pHZ7cYMS+UBmwL2V97juuvBgnw3ZRTHtXAhkE/KXAGV6l8B9xwIaXyXRx4a6Jb+Mf7syiAGfa0WRXbNF9SmBeaTYM07kjJVqyI19OhUOrAXFU/K3O3ybjoXvacDd+kJegsmRxn1GoE2oKFASJpAADaE0a7W9gQC8UgwriJQDHq2T34BKULbmt+nvEvphF+gqOhCcvIS7/e7h/+C41zKTxpFsod3+hoI0rEjAciMm6m59bUk+vJ+2bQXeJ2Y8cbya/KmahJOOlPwbjlNU1qLXx2sjXx4Nm9hhsBvlxxqurqu4vKhdjDpAh33TFiasdr8H4SZkiev/FM26tRLspiOfMJq8BmQHTUQGFv559U17HYzch54pRCKwYBBCuO/wSu5pXIUpPPceAniTxk3IVe8H57xXhoFBo0KakC3cbXrCnwR2/Tqn/AD5xTI4vq7DNmjUYUpxZNAz8lc6Msvq2Rn4NKRDWg9E8dSZuBzAjy7YIGQ8431PPWlvumCluHdt0R4TLH7hV115icDiE/UdoH2SM8ssPDyAFKW1gMGGBHHNAxILGx5lt2ekwwipx+E6trDoCxmgxASNgGqpVbWB6/XrOfLsqvf3nOlC64VJpXnYEaNl0YcRf+V+ScKQwe9MNzR+u+3d0BpHzGTIxmsi+WoAVbhn5ALG9Rd3gTKbcYzX7LrYmQrJPKaCVWjuhxJ+bQ8qk736YhnugijLl5tKrIVhoQIHftqpVi+uAbNsosQpFCUVi/+o55uuWUYXIVh1nJY5GgTs8dplbLkCfCtXPtPJZaj2iick9mJIO6OhR32/hR3JtqO/+wMJyScaH3qB1O+isbUtlyavkvuNHzn0qfrKEXiL4hzVWgPmTZeUIYSdTZ0IHNnYSVC6Rqd6T7SHTzHS1MaXx2ZTUXXHu8H7DyRP0RZTlHo/+5dzLI8VjdCAtQ8hYBTwaZdQmI1ElgEsnnNGcP1mrAFdpSdYwD98qQvZCPbmPdSWv/Ba8YQjlnB1pySwvJH2uuYdGJSydFsQKpJXMbr2c8DHOMGCC3enWNVWmuV7z9FQxTlL2kwn0Ny591JbQ6mHizGXf3F3obq+v+3uqP6o3DcR+Qyo2bGMVXyTW6JwyB7eRuN+YprKVV5obm5I4BAl/InzowMNUj/un6XzFU5JJPDe7ugAA=="
LOGO_DARK_WEBP_B64 = "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"


# 紹介画面に載せる、実際のアプリ画面。
# 中が見えないアプリなので、登録前に本物の画面を見せる。
SHOT_HOME_B64 = "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"
SHOT_FOREST_B64 = "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"
SHOT_PLAZA_B64 = "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"


@app.get("/shot-home.webp")
def get_shot_home():
    return Response(
        content=base64.b64decode(SHOT_HOME_B64),
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=604800"},
    )

@app.get("/shot-forest.webp")
def get_shot_forest():
    return Response(
        content=base64.b64decode(SHOT_FOREST_B64),
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=604800"},
    )

@app.get("/shot-plaza.webp")
def get_shot_plaza():
    return Response(
        content=base64.b64decode(SHOT_PLAZA_B64),
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=604800"},
    )


# リンクをSNSに貼ったときに出るカードの画像（1200x630）。
# JPEGにしてあるのは、X・LINE・Slackなど取得する側の対応が一番広いため。
OGP_JPEG_B64 = "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"


@app.get("/ogp.jpg")
def get_ogp_image():
    return Response(
        content=base64.b64decode(OGP_JPEG_B64),
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=604800"},
    )


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


TERMS_BODY_HTML = '<p>本利用規約（以下「本規約」といいます）は、森の主 OH-GY（以下「運営者」といいます）が提供する学習記録共有サービス「みんスタ」（以下「本サービス」といいます）の利用条件を定めるものです。本サービスを利用するすべての方（以下「ユーザー」といいます）は、本規約に同意のうえ、本サービスを利用するものとします。</p>\n<h2>第1条（本サービスの内容）</h2>\n<p>1. 本サービスは、ユーザーが学習の記録を行い、同じ目標を持つ他のユーザーと少人数のチームを組んで、互いの学習状況を共有しながら学習の継続を支援することを目的としたサービスです。</p>\n<p>2. 本サービスには、ユーザーのチームを構成するために、運営者が用意したAIによる仮想のメンバー（以下「AIメンバー」といいます）が含まれる場合があります。詳細は第8条に定めます。</p>\n<p>3. 本サービスは現在、開発中のベータ版（試験提供）です。ユーザーは、本サービスが完成された製品ではなく、不具合・仕様変更・データの消失等が生じうることを理解したうえで利用するものとします。</p>\n<p>4. 運営者は、本サービスの内容を、ユーザーへの事前の通知なく変更・追加・廃止することがあります。</p>\n<p>5. ユーザーは、塾・学校・サークル等（以下「コミュニティ」といいます）が発行するコードを入力することで、そのコミュニティに所属できます。所属したユーザーのチームは、同じコミュニティに所属するユーザーの中から編成されます。ユーザーは、いつでもコミュニティへの所属を解除できます。</p>\n<h2>第2条（ベータ版であること・データの取り扱い）</h2>\n<p>1. 本サービスはベータ版であるため、運営者は、開発・保守・障害対応・仕様変更等にともない、ユーザーの学習記録その他のデータの全部または一部を、事前の予告なく変更・初期化・削除する場合があります。</p>\n<p>2. ユーザーは、本サービスに記録したデータが永続的に保存されることを保証されないことを、あらかじめ承諾するものとします。重要な記録は、ユーザー自身で別途控えを保管することを推奨します。</p>\n<p>3. データの消失・破損によってユーザーに生じた損害について、運営者は第11条の定めに従い責任を負いません。</p>\n<h2>第3条（利用条件・本規約への同意）</h2>\n<p>1. ユーザーは、本規約に同意した時点で、本サービスを利用できるものとします。</p>\n<p>2. ユーザーが本サービスの新規登録を行う際、または本サービスを実際に利用した時点で、ユーザーは本規約およびプライバシーポリシーの内容を確認し、これらに同意したものとみなされます。</p>\n<p>3. 運営者は、新規登録画面において、本規約およびプライバシーポリシーに同意する旨の明示的な意思表示（チェックボックスへのチェック等）を求めることがあります。この場合、ユーザーは、当該意思表示を行うことで本規約に同意したものとします。</p>\n<p>4. 未成年者が本サービスを利用する場合は、親権者など法定代理人の同意を得たうえで利用するものとします。運営者は、未成年者による利用について、当該同意があったものとして取り扱うことができます。</p>\n<h2>第4条（アカウントの登録・管理・退会）</h2>\n<p>1. ユーザーは、本サービスの利用にあたり、メールアドレス、パスワード、ニックネーム等の必要な情報を、正確かつ最新の内容で登録するものとします。パスワードは8文字以上とします。</p>\n<p>2. ユーザーは、前項に代えて、Googleアカウントを用いて登録・ログインすることができます。この場合、ユーザーはパスワードを設定せず、本サービスへのログインはGoogleアカウントによって行われます。ユーザーが当該Googleアカウントを利用できなくなった場合、本サービスにログインできなくなることがあります。</p>\n<p>3. ユーザーは、登録したパスワードおよびアカウントを、自己の責任において適切に管理するものとし、第三者に利用させ、または貸与・譲渡してはなりません。</p>\n<p>4. パスワードの管理不十分、入力の誤り、第三者の使用等によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<p>5. ニックネームは他のユーザーに表示される場合があります。ユーザーは、本名その他自己を特定できる情報をニックネームに用いないことを推奨されます。</p>\n<p>6. ログインの状態は、ログインした時点から60日が経過すると解除されます。複数の端末で同時にログインすることができ、ログアウトはその端末についてのみ行われます。</p>\n<p>7. 運営者は、不正なログインを防ぐため、一定時間内にログインの失敗が続いた場合、一時的にログインを受け付けないことがあります。</p>\n<p>8. ユーザーは、設定画面から、いつでも退会（アカウントの削除）することができます。退会にあたっては、本人以外による削除を防ぐため、パスワードの再入力、またはGoogleアカウントでの再認証を求めます。退会すると、ユーザーの学習記録、参考書、投稿その他の情報は削除され、元に戻すことはできません。</p>\n<h2>第5条（学習記録・投稿内容の取り扱い）</h2>\n<p>1. ユーザーが本サービスに投稿した学習記録、メッセージ、その他の情報（以下「投稿内容」といいます）は、同じチームのユーザー等、本サービスの仕様に応じた範囲で、他のユーザーに表示される場合があります。</p>\n<p>2. 投稿内容に関する責任は、投稿したユーザーが負うものとします。</p>\n<p>3. 投稿内容の著作権は、ユーザーに留保されます。ただし、ユーザーは運営者に対し、本サービスの提供・維持・改善・品質向上・不具合対応のために必要な範囲で、投稿内容を無償かつ非独占的に利用（複製・保存・集計・分析・表示等）する権利を許諾するものとします。</p>\n<p>4. 運営者は、投稿内容を含む本サービスの利用状況を統計的に集計・分析し、個人を特定できない形に加工したうえで、本サービスの紹介、研究、学業上の発表、就職活動その他の目的で、その分析結果を利用・公表することができるものとします。この場合においても、個人を特定できる情報を公表することはありません。</p>\n<p>5. 個人情報の取り扱いについては、別途定めるプライバシーポリシーによります。</p>\n<p>6. ユーザーがコミュニティに所属した場合でも、運営者は、ユーザーの情報をコミュニティの運営者（塾の先生等）に提供しません。将来、コミュニティの運営者がユーザーの学習状況を閲覧できる機能を提供する場合は、共有する項目を示したうえで、ユーザー（16歳未満の場合はその法定代理人）の同意を得てから開始します。</p>\n<h2>第6条（禁止事項）</h2>\n<p>ユーザーは、本サービスの利用にあたり、次の行為をしてはなりません。</p>\n<p>(1) 法令または公序良俗に違反する行為</p>\n<p>(2) 他のユーザーまたは第三者の権利・名誉・プライバシーを侵害する行為</p>\n<p>(3) 他のユーザーまたは第三者を誹謗中傷し、不快にさせ、または迷惑をかける行為</p>\n<p>(4) わいせつ、暴力的、差別的その他不適切な内容を、ニックネームや投稿内容に用いる行為</p>\n<p>(5) 虚偽の情報を登録または投稿する行為</p>\n<p>(6) 本サービスの運営を妨害する行為、サーバーやネットワークに過度の負荷をかける行為</p>\n<p>(7) 不正アクセス、その他本サービスのセキュリティを脅かす行為</p>\n<p>(8) 本サービスを、本来の目的（学習の記録・継続支援）以外に利用する行為</p>\n<p>(9) 自動化された手段により本サービスに大量のデータを送信し、または情報を取得する行為</p>\n<p>(10) その他、運営者が不適切と判断する行為</p>\n<h2>第7条（利用の制限・チームからの離脱）</h2>\n<p>1. 運営者は、ユーザーが本規約に違反した場合、または違反するおそれがあると判断した場合、事前の通知なく、投稿内容の削除、ニックネームの変更、利用の一時停止、アカウントの削除等の措置を行うことができます。</p>\n<p>2. 前項の措置によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<p>3. 本サービスでは、3日間連続で学習の記録がないユーザーは、チームの席を空けるため、自動的にチームから離脱します。離脱しても学習記録は消えず、ユーザーはいつでもチームに再参加できます。運営者は、この基準を変更する場合、事前に本サービス上で告知します。</p>\n<h2>第8条（AIメンバーについて）</h2>\n<p>1. 本サービスでは、チームの人数を補うため、運営者が用意したAIメンバーがチームに参加することがあります。AIメンバーは実在の人物ではありません。</p>\n<p>2. チームの構成や人数によっては、ユーザー以外のチームのメンバーが、AIメンバーのみとなる場合があります。</p>\n<p>3. AIメンバーの発言・反応は自動的に生成されるものであり、その内容の正確性・適切性について運営者は保証せず、責任を負いません。</p>\n<h2>第9条（料金）</h2>\n<p>1. 本サービスは、現在、無料で提供されています。</p>\n<p>2. 運営者は、将来、本サービスの一部の機能を有料で提供することがあります。その場合の料金・支払方法・その他の条件は、別途定め、事前にユーザーに通知します。</p>\n<h2>第10条（サービスの中断・停止・終了）</h2>\n<p>1. 運営者は、次の場合に、ユーザーへの事前の通知なく、本サービスの全部または一部を中断・停止することができます。</p>\n<p>(1) システムの保守・点検・更新を行う場合</p>\n<p>(2) 火災・停電・天災等の不可抗力により、本サービスの提供が困難な場合</p>\n<p>(3) 利用しているサーバー、ネットワーク、外部サービス等に障害が生じた場合</p>\n<p>(4) その他、運営者が必要と判断した場合</p>\n<p>2. 運営者は、本サービスを終了する場合、合理的な方法でユーザーに通知するよう努めます。ただし、ベータ版であることまたは緊急やむを得ない事情がある場合は、事前の通知なく終了することができます。</p>\n<p>3. 本サービスの中断・停止・終了によってユーザーに生じた損害について、運営者は責任を負いません。</p>\n<h2>第11条（免責事項）</h2>\n<p>1. 本サービスは、個人により、ベータ版として運営されています。運営者は、本サービスの内容・機能・データの保存等について、完全性・正確性・有用性・継続性・特定目的への適合性を、明示・黙示を問わず保証するものではありません。</p>\n<p>2. 運営者は、本サービスの利用または利用できなかったことによってユーザーに生じた損害（不具合、データの消失・破損、サービスの中断・終了、チームからの自動離脱等を含みます）について、責任を負いません。ただし、運営者の故意または重過失による場合は、この限りではありません。</p>\n<p>3. 前項ただし書きにより運営者が責任を負う場合であっても、その賠償の範囲は、現実に発生した通常かつ直接の損害に限り、本サービスが無償で提供されていることに鑑み、適切な範囲に限られるものとします。</p>\n<p>4. ユーザー間またはユーザーと第三者との間で生じた紛争について、運営者は責任を負いません。ユーザーは、自己の責任と費用において当該紛争を解決するものとします。</p>\n<h2>第12条（本規約の変更）</h2>\n<p>1. 運営者は、必要と判断した場合、本規約を変更することができます。重要な変更を行う場合は、合理的な方法でユーザーに通知または本サービス上に表示します。</p>\n<p>2. 変更後の本規約は、本サービス上に表示された時点から効力を生じるものとします。変更後にユーザーが本サービスを利用した場合、変更後の本規約に同意したものとみなされます。</p>\n<h2>第13条（準拠法・管轄）</h2>\n<p>1. 本規約の解釈・適用は、日本法を準拠法とします。</p>\n<p>2. 本サービスに関して運営者とユーザーとの間で紛争が生じた場合には、運営者の所在地を管轄する裁判所を、第一審の専属的合意管轄裁判所とします。</p>\n<h2>第14条（お問い合わせ・運営者）</h2>\n<p>本サービスの運営者、および本規約に関するお問い合わせ先は、以下のとおりです。</p>\n<p>運営者：森の主 OH-GY</p>\n<p>連絡先：ohgy.dev@gmail.com</p>\n<p>制定日：2026年6月27日</p>\n<p>改定日：2026年10月18日</p>\n<p>以上</p>'
PRIVACY_BODY_HTML = '<p>森の主 OH-GY（以下「運営者」といいます）は、学習記録共有サービス「みんスタ」（以下「本サービス」といいます）におけるユーザーの個人情報・データの取り扱いについて、以下のとおりプライバシーポリシー（以下「本ポリシー」といいます）を定めます。本サービスはベータ版（試験提供）です。</p>\n<h2>第1条（取得する情報）</h2>\n<p>運営者は、本サービスの提供にあたり、次の情報を取得します。</p>\n<p>(1) ユーザーが登録時・利用時に入力する情報</p>\n<p>・メールアドレス</p>\n<p>・パスワード（第4条のとおり、元に戻せない形に変換（ハッシュ化）して保存します）</p>\n<p>・ニックネーム</p>\n<p>・学習の目標、目標日</p>\n<p>・プロフィール画像（ユーザーが設定した場合）</p>\n<p>・所属するコミュニティ（ユーザーがコミュニティのコードを入力した場合）</p>\n<p>(2) ユーザーがGoogleアカウントで登録・ログインする場合に、Google LLCから受け取る情報</p>\n<p>・メールアドレス</p>\n<p>・同社におけるユーザーの識別子（アカウントを結びつけるために保存します）</p>\n<p>・メールアドレスが同社において確認済みであるかどうか</p>\n<p>なお、同社からはユーザーの表示名も送られてきますが、運営者はこれを保存しません。ニックネームは、他のユーザーに表示されるものであるため、ユーザー自身に決めていただきます。</p>\n<p>(3) ユーザーが本サービスの利用にともない生成する情報</p>\n<p>・学習記録（学習内容、学習時間、記録日時など）</p>\n<p>・登録した参考書等の情報</p>\n<p>・その日ごとの目標（宣言）などの情報</p>\n<p>・チーム内で送信したメッセージ、リアクション</p>\n<p>・チームへの所属状況、学習の継続状況など</p>\n<p>(4) サービスの提供に必要な技術的な情報</p>\n<p>・ログインの状態を保つための情報（ハッシュ化したトークンと有効期限）</p>\n<p>・ログインの試行を制限するための情報（IPアドレスとメールアドレス。ログインに失敗した場合に限り、サーバーのメモリ上に最大15分間保持し、データベースには保存しません）</p>\n<p>・プッシュ通知の送信に必要な購読情報（ユーザーが通知を有効にした場合）</p>\n<p>・25分集中の終了を知らせる通知の予約（ユーザーが25分集中を開始した場合）</p>\n<p>・登録のきっかけになった経路（広告やSNSなど。リンクに付けた短い印だけで、個人を特定する情報は含みません）</p>\n<p>(5) ユーザーの端末内に保存する情報</p>\n<p>本サービスは、次の情報をユーザーの端末（ブラウザ）内に保存します。このうち、ログインの状態を示す情報以外は、運営者に送信されません。</p>\n<p>・ログインの状態を示す情報\u3000・表示の設定（ダークテーマ等）\u3000・計測中のタイマーの状態\u3000・案内を表示したかどうかの記録</p>\n<h2>第2条（利用目的）</h2>\n<p>運営者は、取得した情報を、次の目的のために利用します。</p>\n<p>(1) 本サービスの提供・維持・運営のため</p>\n<p>(2) ユーザーのチーム編成、学習記録の表示、チーム内での共有など、本サービスの機能を提供するため</p>\n<p>(3) ユーザーが有効にした場合に、学習を促すプッシュ通知や、25分集中の終了の通知を送信するため</p>\n<p>(4) 本サービスの不具合対応、安全性の確保、不正なログインや不正利用の防止のため</p>\n<p>(5) 本サービスの改善・品質向上のため（広告やSNSなど、どの経路からの登録が多いかの把握を含みます）</p>\n<p>(6) 本サービスの利用状況を統計的に分析し、個人を特定できない形に加工したうえで、本サービスの紹介・研究・学業上の発表・就職活動等に利用するため</p>\n<p>(7) ユーザーからのお問い合わせに対応するため</p>\n<p>(8) その他、上記に付随する目的のため</p>\n<h2>第3条（統計データ・匿名加工情報の利用）</h2>\n<p>1. 運営者は、取得した情報を統計的に集計・分析し、特定の個人を識別できないように加工した情報（以下「統計データ」といいます）を作成することがあります。</p>\n<p>2. 運営者は、統計データを、本サービスの紹介・改善、研究、学業上の発表、就職活動その他の目的で、利用・公表することができます。統計データには、特定の個人を識別できる情報（メールアドレス、ニックネーム等）は含めません。</p>\n<h2>第4条（パスワード・ログイン情報の取り扱い）</h2>\n<p>1. ユーザーのパスワードは、元に戻せない形に変換（ハッシュ化）した状態で保存します。運営者は、ユーザーの生のパスワードを保持しません。</p>\n<p>2. ログインの状態を保つためのトークンも、ハッシュ化した状態で保存し、発行から60日で失効させます。</p>\n<p>3. Googleアカウントでのみ登録したユーザーについては、運営者はパスワードを設定せず、保持しません。この場合、ログインおよび退会時の本人確認は、Googleアカウントでの認証によって行います。</p>\n<h2>第5条（情報の共有・第三者への提供）</h2>\n<p>1. ユーザーが本サービスに入力・投稿した情報のうち、ニックネーム、学習記録、メッセージ等は、本サービスの仕様に応じて、同じチームのユーザー等、他のユーザーに表示される場合があります。メールアドレスやパスワードが他のユーザーに表示されることはありません。</p>\n<p>2. 運営者は、次の場合を除き、ユーザーの個人情報を第三者に提供しません。</p>\n<p>(1) ユーザーの同意がある場合</p>\n<p>(2) 法令に基づき開示が必要な場合</p>\n<p>(3) 人の生命・身体・財産の保護のために必要であり、本人の同意を得ることが困難な場合</p>\n<p>3. ユーザーがコミュニティに所属した場合でも、運営者は、ユーザーの情報をコミュニティの運営者（塾の先生等）に提供しません。所属によって変わるのは、チームを編成する範囲だけです。将来、コミュニティの運営者がユーザーの学習状況を閲覧できる機能を提供する場合は、共有する項目と共有しない項目を示したうえで、ユーザー（16歳未満の場合はその法定代理人）の同意を得てから開始します。</p>\n<h2>第6条（外部サービスの利用）</h2>\n<p>1. 本サービスは、参考書等の書籍情報の検索機能のため、Google LLCが提供する「Google Books API」を利用しています。ユーザーが書籍を検索する際、入力された検索語句が同社に送信され、検索結果として書籍情報を取得します。同社における情報の取り扱いは、同社の定めるプライバシーポリシー（https://policies.google.com/privacy 等）によります。</p>\n<p>2. 本サービスは、Googleアカウントでのログイン機能のため、Google LLCが提供する「Google Identity Services」を利用しています。ユーザーが「Googleでログイン」を選んだ場合、ユーザーの端末から同社へ接続され、同社の画面でアカウントを選択したうえで、本サービスは同社が発行した認証情報（第1条(2)の情報を含みます）を受け取ります。ユーザーのGoogleアカウントのパスワードが本サービスに渡ることはありません。同社における情報の取り扱いは、同社の定めるプライバシーポリシー（https://policies.google.com/privacy 等）によります。この機能を使わない場合、同社への接続は行われません。</p>\n<p>3. 本サービスの画面は、表示に使う文字（Google LLCの「Google Fonts」）、グラフの描画に使うプログラム（jsDelivrが配信する「Chart.js」）、環境音（mixkit および Wikimedia Commons が配信する音声ファイル）を、ユーザーの端末から直接読み込みます。その際、ユーザーのIPアドレスやブラウザの情報が、それぞれの提供元に送信されます。</p>\n<p>4. 本サービスのサーバーおよびデータベースは、米国の事業者であるRender（https://render.com）が提供するサービスを利用しています。ユーザーのデータは、同社のサーバー上に保存されます。</p>\n<p>5. 本サービスのプッシュ通知は、ブラウザおよびOSの提供する配信の仕組みを通じて送信されます。</p>\n<p>6. 本サービスは、第三者によるアクセス解析ツールおよび第三者の広告サービスを使用していません。</p>\n<h2>第7条（情報の管理・保護）</h2>\n<p>運営者は、取得した情報の漏えい・滅失・毀損の防止その他の安全管理のために、必要かつ適切な措置を講じるよう努めます。ただし、本サービスは個人によりベータ版として運営されており、運営者は情報の安全性について完全性を保証するものではありません。</p>\n<h2>第8条（情報の保存期間・削除）</h2>\n<p>1. 運営者は、利用目的の達成に必要な範囲で、ユーザーの情報を保存します。主な情報の保存期間は次のとおりです。</p>\n<p>・ログインの状態：発行から60日で失効し、削除します</p>\n<p>・ログインの試行の記録：最大15分間（サーバーのメモリ上のみ）</p>\n<p>・その日ごとの目標（宣言）：30日を過ぎたものを自動で削除します</p>\n<p>・25分集中の終了通知の予約：通知した時点、または取り消した時点で削除します</p>\n<p>2. 本サービスはベータ版であるため、運営者は、開発・保守等にともない、ユーザーの情報の全部または一部を、事前の予告なく削除・初期化する場合があります。</p>\n<p>3. ユーザーは、設定画面から、いつでも退会（アカウントの削除）することができます。退会すると、運営者は、アカウント、学習記録、参考書、その日ごとの目標、メッセージ、リアクション（ユーザーのメッセージに他のユーザーが付けたものを含みます）、プッシュ通知の購読情報、通知の予約、ログインの状態を、直ちに削除します。ただし、障害に備えた控え（バックアップ）等に、一定期間残る場合があります。</p>\n<p>4. 前項にかかわらず、個人を特定できない形に加工された統計データは、退会後も保持・利用されることがあります。</p>\n<p>5. ユーザーは、自己の情報の開示・訂正・削除等を希望する場合、第10条のお問い合わせ先に連絡することができます。運営者は、本人からの請求であることを確認のうえ、法令に従い、合理的な範囲で対応します。</p>\n<h2>第9条（未成年のユーザー）</h2>\n<p>1. 未成年のユーザーは、親権者など法定代理人の同意を得たうえで、本サービスを利用してください（利用規約第3条）。</p>\n<p>2. 16歳未満のユーザー本人またはその法定代理人は、前条第5項の方法により、ユーザーの情報の削除等を請求することができます。</p>\n<h2>第10条（お問い合わせ先）</h2>\n<p>本ポリシーに関するお問い合わせ、および個人情報の取り扱いに関するご請求は、以下までご連絡ください。</p>\n<p>運営者：森の主 OH-GY</p>\n<p>連絡先：ohgy.dev@gmail.com</p>\n<h2>第11条（本ポリシーの変更）</h2>\n<p>1. 運営者は、必要と判断した場合、本ポリシーを変更することがあります。重要な変更を行う場合は、本サービス上で告知します。</p>\n<p>2. 変更後の本ポリシーは、本サービス上に表示された時点から効力を生じるものとします。</p>\n<p>制定日：2026年6月27日</p>\n<p>改定日：2026年10月18日</p>\n<p>以上</p>'


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
def register(request: Request, user_data: dict, db: Session = Depends(get_db)):
    # 同じ相手が自動で大量のアカウントを作るのを防ぐ
    check_register_rate(request)
    # 必須項目の欠落・空文字を検証する(未検証だと KeyError で 500 になる)。
    email = (user_data.get("email") or "").strip()
    password = user_data.get("password") or ""
    name = need_text(user_data.get("name"), "お名前", MAX_NAME)
    goal = need_text(user_data.get("goal"), "目標", MAX_GOAL)
    if not email or not password or not name or not goal:
        raise HTTPException(status_code=400, detail="必須項目が入力されていません")
    if len(email) > 254:
        raise HTTPException(status_code=400, detail="メールアドレスが長すぎます")
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
    target_date = need_date(user_data.get("target_date"), "目標の期日")

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
            target_date=target_date,
            signup_source=clean_source(user_data.get("src")),
            created_at=utcnow(),
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        record_register(request)
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
    # Googleだけで登録した人は hashed_password が NULL。
    # NULL に .encode() を呼ぶと 500 で落ちるので、先に確かめて案内に振り分ける。
    if user and not user.hashed_password:
        raise HTTPException(
            status_code=400,
            detail="このメールアドレスはGoogleで登録されています。「Googleでログイン」からお進みください",
        )
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


@app.get("/auth/google/client-id")
def get_google_client_id():
    """画面がGoogleのボタンを出すために使うクライアントID。
    未設定なら無効を伝え、画面はボタンを出さない（設定なしでもアプリは動く）。"""
    return JSONResponse(
        {"enabled": GOOGLE_LOGIN_ENABLED, "client_id": GOOGLE_CLIENT_ID}
    )


def _google_session(db: Session, user: User, is_new: bool):
    """Googleでの認証が済んだ人に、セッションを発行して返す。"""
    return {
        "status": "ok",
        "is_new": is_new,
        "user": {"id": user.id, "name": user.name, "goal": user.goal},
        "token": create_session(db, user),
    }


@app.post("/auth/google")
def auth_google(request: Request, data: dict, db: Session = Depends(get_db)):
    """Googleでログイン、または新規登録する。

    3つの場合がある。
      ・google_sub が一致する人がいる  → その人としてログイン
      ・同じメールアドレスの人がいる    → その人に google_sub を結びつけてログイン
                                       （パスワードは残るので、以降どちらでも入れる）
      ・どちらもいない                → まだ作らず、目標と名前を聞き返す

    最後の場合でアカウントをすぐ作らないのは、目標が空の人をDBに残さないため。
    チーム編成は目標で決まるので、空の人は宙に浮く。
    IDトークンは1時間ほど有効なので、画面が持っておいて目標と一緒に送り直せばよい。
    """
    check_ip_rate(request)
    try:
        info = verify_google_credential(data.get("credential") or "")
    except HTTPException:
        record_ip_failure(request)
        raise

    # 本人を示す情報は、必ず検証済みの中身から取る（画面から来た値は使わない）。
    sub = info["sub"]
    email = (info.get("email") or "").strip()

    # すでにGoogleで入ったことがある人
    user = db.query(User).filter(User.google_sub == sub).first()
    if user:
        return _google_session(db, user, is_new=False)

    # パスワードで登録済みの人（同じメールアドレス）に結びつける
    user = db.query(User).filter(User.email == email).first()
    if user:
        # すでに別のGoogleアカウントが結びついている場合は上書きしない。
        # 上書きを許すと、同じメールアドレスを持つ別のGoogleアカウントで
        # 既存の利用者を乗っ取れてしまう。
        if user.google_sub and user.google_sub != sub:
            raise HTTPException(
                status_code=400,
                detail="このメールアドレスは別のGoogleアカウントに結びついています",
            )
        user.google_sub = sub
        db.commit()
        return _google_session(db, user, is_new=False)

    # ここから新規。目標と名前が揃うまでは作らない。
    if not (data.get("name") or "").strip() or not (data.get("goal") or "").strip():
        return JSONResponse({"status": "need_goal"})
    name = need_text(data.get("name"), "お名前", MAX_NAME)
    goal = need_text(data.get("goal"), "目標", MAX_GOAL)
    target_date = need_date(data.get("target_date"), "目標の期日")
    # 新規なのでここで初めてアカウントが増える。連打を防ぐ。
    check_register_rate(request)

    new_user = None
    try:
        new_user = User(
            # IDはパスワード登録と同じ採番を通す（人間の連番を崩さないため）
            id=get_custom_id(db, is_ai=False),
            email=email,
            hashed_password=None,   # Googleだけで入る人はパスワードを持たない
            google_sub=sub,
            name=name,
            goal=goal,
            target_date=target_date,
            signup_source=clean_source(data.get("src")),
            created_at=utcnow(),
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        record_register(request)
        # チーム割り当てで失敗すると、目標だけ持った人が宙に浮いて残るため、
        # 失敗したら作ったユーザーを消して元に戻す（パスワード登録と同じ後始末）。
        assign_group_logic(db, new_user)
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"google register failed: {e}")
        if new_user is not None:
            try:
                saved = db.query(User).filter(User.id == new_user.id).first()
                if saved is not None:
                    db.delete(saved)
                    db.commit()
            except Exception as cleanup_error:
                db.rollback()
                print(f"google register cleanup failed: {cleanup_error}")
        raise HTTPException(status_code=500, detail="登録に失敗しました。もう一度お試しください")
    return _google_session(db, new_user, is_new=True)


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
        # 退会の確認でパスワードを聞くか、Googleで認証させるかを画面が選ぶために使う。
        # google_sub そのものは本人を示す値なので返さない。
        "has_password": bool(current_user.hashed_password),
        "has_google": bool(current_user.google_sub),
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
    new_goal = need_text(goal_data.get("goal"), "目標", MAX_GOAL)
    new_target_date = need_date(goal_data.get("target_date"), "目標の期日")
    # 目標が変わった場合のみグループを再編成する。期日(target_date)だけの
    # 変更ではグループを維持する(同じ目標の仲間と並走を続けられるように)。
    goal_changed = (new_goal != old_goal)
    user.goal = new_goal
    user.target_date = new_target_date
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
    # 画面側は必ず100×100のJPEGに変換してから送っている。
    # ここで形と大きさを確かめないと、巨大な文字列をそのまま保存できてしまう。
    user.profile_image = need_image(
        image_data.get("profile_image"), "プロフィール画像", MAX_PROFILE_IMAGE
    )
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
    user.name = need_text(name_data.get("name"), "ニックネーム", MAX_NAME)
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
    # 宛先はサーバーが実際に接続しに行くURLなので、必ず確かめる
    endpoint = clean_push_endpoint(sub_data.get("endpoint"))
    keys = sub_data.get("keys") or {}
    if not isinstance(keys, dict):
        raise HTTPException(status_code=400, detail="購読情報が不正です")
    p256dh = need_text(keys.get("p256dh"), "通知の鍵", MAX_PUSH_FIELD)
    auth = need_text(keys.get("auth"), "通知の鍵", MAX_PUSH_FIELD)
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
    endpoint = need_text(
        sub_data.get("endpoint"), "通知の宛先", MAX_PUSH_FIELD, required=False
    )
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
    本人確認を求める（ログイン中の端末を他人に触られても、
    それだけでは消せないようにするため）。確認の手段は人によって違う。
      ・パスワードを持つ人   → パスワードの再入力
      ・Googleだけの人       → もう一度Googleで認証（hashed_password が NULL）
      ・両方持つ人           → どちらでもよい

    消すもの: 本人の学習記録・参考書・種・投稿・スタンプ・通知の宛先・
    通知予約・セッション、そして本人のアカウントそのもの。
    本人の投稿に他の人が付けたスタンプも、投稿と一緒に消える。
    抜けたあとのチームは、残った人のために3人に整え直す。
    """
    password = data.get("password") or ""
    credential = data.get("credential") or ""
    check_login_rate(request, current_user.email)

    # NULL の hashed_password に .encode() を呼ばないこと。
    # これを忘れると、Googleだけの人の退会が 500 で落ち、
    # 規約で約束した「いつでも退会できる」が守れなくなる。
    ok = False
    if password and current_user.hashed_password:
        ok = bcrypt.checkpw(
            password.encode("utf-8"), current_user.hashed_password.encode("utf-8")
        )
    if not ok and credential and current_user.google_sub:
        try:
            ok = verify_google_credential(credential).get("sub") == current_user.google_sub
        except HTTPException:
            ok = False

    if not ok:
        record_login_failure(request, current_user.email)
        # 401 は画面側で「ログイン切れ」として扱われ、強制ログアウトになるため 400 で返す
        if current_user.hashed_password:
            raise HTTPException(status_code=400, detail="パスワードが違います")
        raise HTTPException(
            status_code=400, detail="Googleの確認ができませんでした。もう一度お試しください"
        )
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
    code = need_text(data.get("code"), "塾コード", 64).upper()
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
    # 1人が本棚を無限に増やせないようにする
    if db.query(Book).filter(Book.user_id == user_id).count() >= MAX_BOOKS_PER_USER:
        raise HTTPException(
            status_code=400, detail=f"参考書は{MAX_BOOKS_PER_USER}冊までです"
        )
    new_book = Book(
        user_id=user_id,
        title=need_text(book_data.get("title"), "参考書の題名", MAX_BOOK_TITLE),
        color=need_color(book_data.get("color")),
        # 表紙はGoogleブックスの画像URLのこともあるので、URLも許す
        cover_image=need_image(
            book_data.get("cover_image"), "表紙画像", MAX_COVER_IMAGE, allow_url=True
        ),
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
        book.color = need_color(book_data.get("color"))
    if book_data.get("cover_image"):
        book.cover_image = need_image(
            book_data.get("cover_image"), "表紙画像", MAX_COVER_IMAGE, allow_url=True
        )

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

    # 参考書を指定するなら、自分の本棚にあるものに限る。
    # 確かめないと、他人の参考書の番号を自分の記録に結び付けられる。
    book_id = report_data.get("book_id")
    if book_id is not None:
        if not isinstance(book_id, int) or isinstance(book_id, bool):
            raise HTTPException(status_code=400, detail="参考書の指定が正しくありません")
        owned = (
            db.query(Book)
            .filter(Book.id == book_id, Book.user_id == user.id)
            .first()
        )
        if not owned:
            raise HTTPException(status_code=400, detail="その参考書は見つかりません")

    r = Report(
        user_id=user.id,
        book_id=book_id,
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

    # 画面は決まったスタンプしか送らないが、APIを直接叩けば何でも送れる。
    # 長さを限らないと、1件のスタンプで大きな文字列を保存されてしまう。
    emoji = need_text(data.get("emoji"), "スタンプ", MAX_EMOJI)

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
    # 先に入力を確かめる。設定の有無より、送られてきた値の検証を優先する。
    # Googleへの代理通信なので、無制限な語を投げさせない。
    q = need_text(q, "検索語", MAX_SEARCH_QUERY)
    if not (0 <= startIndex <= 1000):
        raise HTTPException(status_code=400, detail="検索位置が範囲外です")
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