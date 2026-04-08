import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from database.models import Base

# 預設使用 SQLite，未來可改為 PostgreSQL 連線字串
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/hkjc_racing.db")

engine = create_engine(
    DATABASE_URL, 
    echo=False,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)

# 建立 Session 工廠
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def init_db():
    """初始化資料庫表結構"""
    Base.metadata.create_all(engine)

def get_session():
    """獲取資料庫 Session"""
    return Session()
