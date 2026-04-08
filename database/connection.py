import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from database.models import Base

# 預設使用 SQLite，未來可改為 PostgreSQL 連線字串
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/hkjc_racing.db")

# 修正 Railway 的 postgres:// 網址為 postgresql:// (SQLAlchemy 2.0 要求)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL, 
    echo=False,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)

# 建立 Session 工廠
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def init_db():
    """初始化資料庫表結構並預填權重"""
    Base.metadata.create_all(engine)
    
    # 自動預填權重配置 (如果權重表為空)
    from database.models import ScoringWeight
    session = Session()
    try:
        if session.query(ScoringWeight).count() == 0:
            from scripts.init_db import populate_default_weights
            populate_default_weights()
    except Exception as e:
        print(f"預填權重失敗: {e}")
    finally:
        session.close()

def get_session():
    """獲取資料庫 Session"""
    return Session()
