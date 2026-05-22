from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
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
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
import random
import bcrypt
import asyncio


# --- 時刻ユーティリティ ---
# DB には従来どおり「タイムゾーン情報を持たない UTC（naive UTC）」を保存する。
# datetime.utcnow() は Python 3.12 以降で非推奨のため、同じ値を返す関数に置き換える。
# これにより保存される値・既存データとの互換性は完全に維持される。
def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --- データベース設定 ---
engine = create_engine(
    "sqlite:///./minsta.db", connect_args={"check_same_thread": False}
)
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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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
        if (
            group_id in self.active_connections
            and not self.active_connections[group_id]
        ):
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


def adjust_group_members(db: Session, group_id: int, goal: str):
    if not group_id:
        return
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
        db.delete(ai_to_remove)
        total -= 1
    db.commit()


def assign_group_logic(db: Session, user: User):
    potential_groups = db.query(Group).filter(Group.goal == user.goal).all()
    target_group = None
    for g in potential_groups:
        humans = db.query(User).filter(User.group_id == g.id, User.is_ai == False).all()
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
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    assign_group_logic(db, new_user)
    return {"user": {"id": new_user.id, "name": new_user.name, "goal": new_user.goal}}


@app.post("/users/login")
def login_user(login_data: dict, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == login_data["email"]).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not bcrypt.checkpw(
        login_data["password"].encode("utf-8"), user.hashed_password.encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return {"user": {"id": user.id, "name": user.name, "goal": user.goal}}


@app.post("/users/{user_id}/goal")
async def update_goal(user_id: int, goal_data: dict, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    # 改良（バグ修正）: 従来はユーザーが見つからない場合 None を参照して 500 エラーで
    # クラッシュしていた。存在しない場合は 404 を返す。正常系の挙動は完全に不変。
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


@app.post("/users/{user_id}/profile_image")
async def update_profile_image(
    user_id: int, image_data: dict, db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.profile_image = image_data.get("profile_image")
    db.commit()
    if user.group_id:
        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Profile image updated"}


@app.get("/users/")
def get_users(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [
        {
            "id": u.id,
            "name": u.name,
            "goal": u.goal,
            "group_id": u.group_id,
            "target_date": u.target_date,
            "strike_count": u.strike_count,
            "profile_image": u.profile_image,
        }
        for u in users
    ]


@app.get("/groups/{group_id}/members")
def get_members(group_id: int, db: Session = Depends(get_db)):
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
            }
        )
    return res


@app.get("/users/{user_id}/stats")
def get_stats(user_id: int, db: Session = Depends(get_db)):
    # 改良: 合計の算出を DB 側の集計に委ねる（返却値は従来と同一）。
    total = (
        db.query(func.coalesce(func.sum(Report.study_minutes), 0))
        .filter(Report.user_id == user_id)
        .scalar()
    )
    return {"total_minutes": int(total or 0)}


@app.get("/users/{user_id}/books")
def get_books(user_id: int, db: Session = Depends(get_db)):
    books = db.query(Book).filter(Book.user_id == user_id).all()
    return [
        {"id": b.id, "title": b.title, "color": b.color, "cover_image": b.cover_image}
        for b in books
    ]


@app.post("/users/{user_id}/books")
def add_book(user_id: int, book_data: dict, db: Session = Depends(get_db)):
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
def update_book(book_id: int, book_data: dict, db: Session = Depends(get_db)):
    book = db.query(Book).filter(Book.id == book_id).first()
    if not book:
        return {"message": "Book already deleted"}

    if "color" in book_data:
        book.color = book_data["color"]
    if "cover_image" in book_data and book_data["cover_image"]:
        book.cover_image = book_data["cover_image"]

    db.commit()
    return {"message": "Book updated"}


@app.delete("/books/{book_id}")
async def delete_book(book_id: int, db: Session = Depends(get_db)):
    book = db.query(Book).filter(Book.id == book_id).first()
    if not book:
        return {"message": "Book already deleted"}
    user_id = book.user_id
    db.query(Report).filter(Report.book_id == book_id).update({"book_id": None})
    db.delete(book)
    db.commit()
    user = db.query(User).filter(User.id == user_id).first()
    if user and user.group_id:
        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Book deleted"}


@app.post("/reports/submit")
async def submit_report(report_data: dict, db: Session = Depends(get_db)):
    r = Report(
        user_id=report_data["user_id"],
        book_id=report_data.get("book_id"),
        content=report_data["content"],
        study_minutes=report_data["study_minutes"],
    )
    db.add(r)

    user = db.query(User).filter(User.id == report_data["user_id"]).first()
    if user:
        user.strike_count = 0
    db.commit()

    if user and user.group_id:
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
        if ai_members and random.random() < 0.7:
            ai = random.choice(ai_members)
            ai_msg = Message(
                group_id=user.group_id,
                user_id=ai.id,
                content=f"{user.name}さん、{random.choice(AI_ENCOURAGEMENTS)}",
            )
            db.add(ai_msg)
            db.commit()

        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Report submitted"}


@app.get("/users/{user_id}/reports")
def get_reports(user_id: int, db: Session = Depends(get_db)):
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
async def delete_report(report_id: int, user_id: int, db: Session = Depends(get_db)):
    report = (
        db.query(Report)
        .filter(Report.id == report_id, Report.user_id == user_id)
        .first()
    )
    if not report:
        return {"message": "Report already deleted"}
    db.delete(report)
    db.commit()
    user = db.query(User).filter(User.id == user_id).first()
    if user and user.group_id:
        await manager.broadcast_to_group(user.group_id, "update")
    return {"message": "Report deleted"}


@app.get("/groups/{group_id}/messages")
def get_messages(
    group_id: int,
    user_id: Optional[int] = None,
    db: Session = Depends(get_db),
):
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

        # 絵文字ごとに件数を集計し、リクエストしたユーザー自身が押したかどうかも返す。
        agg: Dict[str, dict] = {}
        for rx in reactions_by_msg.get(m.id, []):
            entry = agg.setdefault(
                rx.emoji, {"emoji": rx.emoji, "count": 0, "mine": False}
            )
            entry["count"] += 1
            if user_id is not None and rx.user_id == user_id:
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
async def post_message(group_id: int, msg_data: dict, db: Session = Depends(get_db)):
    msg = Message(
        group_id=group_id, user_id=msg_data["user_id"], content=msg_data["content"]
    )
    db.add(msg)
    db.commit()
    await manager.broadcast_to_group(group_id, "update")
    return {"message": "Message posted"}


# ✨ 新機能: メッセージへの応援リアクション（スタンプ）をトグルする。
@app.post("/messages/{message_id}/reactions")
async def toggle_reaction(message_id: int, data: dict, db: Session = Depends(get_db)):
    msg = db.query(Message).filter(Message.id == message_id).first()
    if not msg:
        raise HTTPException(status_code=404, detail="Message not found")

    user_id = data["user_id"]
    emoji = data["emoji"]

    # 同じユーザー・同じ絵文字が既にあれば取り消し（トグル）、なければ追加する。
    existing = (
        db.query(Reaction)
        .filter(
            Reaction.message_id == message_id,
            Reaction.user_id == user_id,
            Reaction.emoji == emoji,
        )
        .first()
    )
    if existing:
        db.delete(existing)
    else:
        db.add(Reaction(message_id=message_id, user_id=user_id, emoji=emoji))
    db.commit()

    await manager.broadcast_to_group(msg.group_id, "update")
    return {"message": "Reaction toggled"}
