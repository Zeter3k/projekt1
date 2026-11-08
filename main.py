from typing import Annotated, Optional

from datetime import datetime

from fastapi import FastAPI, WebSocket, Depends, WebSocketDisconnect
from fastapi.concurrency import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from sqlmodel import JSON, Field, Session, SQLModel, create_engine, select

class Note(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    author: str
    creation_date: Optional[str] = Field(default=datetime.now().isoformat(), sa_column=Field(JSON))
    last_edit_date: Optional[str] = Field(default=datetime.now().isoformat(), sa_column=Field(JSON))
    
sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, connect_args=connect_args)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session

SessionDep = Annotated[Session, Depends(get_session)]

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# @app.on_event("startup")
# def on_startup():
#     create_db_and_tables()


@app.get("/")
async def get():
    return "Hello, Notes!"

@app.get("/api/getNotes")
async def get_notes():
    with Session(engine) as session:
        notes = session.exec(select(Note)).all()
        return notes

@app.get("/api/getNote/{note_id}")
async def get_note(note_id: int):
    with Session(engine) as session:
        note = session.get(Note, note_id)
        if note:
            return note
        else:
            return {"message": "Note not found"}

@app.post("/api/addNote")
async def add_note(note: Note, session: SessionDep):
    session.add(note)
    session.commit()
    session.refresh(note)

    with open(f"notes/{note.id}.txt", "w") as f:
        f.write("\n")

    return note

@app.post("/api/deleteNote")
async def delete_note(note_id: int, session: SessionDep):
    note = session.get(Note, note_id)
    if note:
        session.delete(note)
        session.commit()
        return {"message": "Note deleted successfully"}
    else:
        return {"message": "Note not found"}



class ConnectionManager:

    def __init__(self):
        self.active_connections: dict[int, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, note_id: int):
        await websocket.accept()

        if note_id not in self.active_connections:
            self.active_connections[note_id] = []

        self.active_connections[note_id].append(websocket)

    def disconnect(self, websocket: WebSocket, note_id: int):
        if note_id in self.active_connections:
            if websocket in self.active_connections[note_id]:
                self.active_connections[note_id].remove(websocket)

            if not self.active_connections[note_id]:
                del self.active_connections[note_id]

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def broadcast(self, message: dict, note_id: int, sender: WebSocket):
        connections = self.active_connections.get(note_id, []).copy()

        for connection in connections:
            if connection is not sender:
                try:
                    await connection.send_json(message)
                except Exception:
                    self.disconnect(connection, note_id)


manager = ConnectionManager()

@app.websocket("/ws/editNote/{note_id}")
async def websocket_endpoint(websocket: WebSocket, note_id: int):

    await manager.connect(websocket, note_id)

    try:
        with open(f"notes/{note_id}.txt", "r", encoding="utf-8") as f:
            content = f.read()

        await manager.send_personal_message({
            "type": "initial",
            "content": content,
            "selection": {
                "start": 0,
                "end": 0,
                "direction": "none"
            }   
        }, websocket)

        while True:
            data = await websocket.receive_json()

            if data["type"] == "content":
                content = data["content"]

                with open(f"notes/{note_id}.txt", "w", encoding="utf-8") as f:
                    f.write(content)

                await manager.broadcast({
                    "type": "content",
                    "content": content,
                    "nickname": data.get("nickname", ""),
                    "selection": data.get("selection", {})
                }, note_id, websocket)

                with Session(engine) as session:
                    note = session.get(Note, note_id)
                    if note:
                        note.last_edit_date = datetime.now().isoformat()
                        session.add(note)
                        session.commit()

            elif data["type"] == "selection":

                await manager.broadcast({
                    "type": "selection",
                    "nickname": data.get("nickname", ""),
                    "selection": data["selection"]
                }, note_id, websocket)

    except WebSocketDisconnect:
        manager.disconnect(websocket, note_id)
