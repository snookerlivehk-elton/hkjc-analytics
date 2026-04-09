import os
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker, scoped_session
from database.models import Base

# 預設使用 SQLite，未來可改為 PostgreSQL 連線字串
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/hkjc_racing.db")

# 修正 Railway 的 postgres:// 網址為 postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# 生產環境 (Postgres) 需要 SSL 設定
connect_args = {}
if "postgresql" in DATABASE_URL:
    connect_args = {"sslmode": "require"}
elif "sqlite" in DATABASE_URL:
    connect_args = {"check_same_thread": False}

engine = create_engine(
    DATABASE_URL, 
    echo=False,
    connect_args=connect_args
)

# 建立 Session 工廠
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def init_db():
    """初始化資料庫表結構並預填權重"""
    Base.metadata.create_all(engine)

    try:
        inspector = inspect(engine)
        cols = {c["name"] for c in inspector.get_columns("horse_histories")}
        if "surface" not in cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE horse_histories ADD COLUMN surface VARCHAR(20)"))
            inspector = inspect(engine)
            cols2 = {c["name"] for c in inspector.get_columns("horse_histories")}
            if "surface" in cols2:
                cols = cols2
                try:
                    with engine.begin() as conn:
                        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_horse_histories_surface ON horse_histories (surface)"))
                except Exception:
                    pass
        if "surface" in cols:
            try:
                with engine.begin() as conn:
                    conn.execute(text("UPDATE horse_histories SET surface='泥地' WHERE (surface IS NULL OR surface='') AND (venue LIKE '%全天候%' OR venue LIKE '%泥地%' OR venue LIKE '%AW%')"))
                    conn.execute(text("UPDATE horse_histories SET surface='草地' WHERE (surface IS NULL OR surface='') AND (venue LIKE '%草地%' OR venue LIKE '%TURF%')"))
            except Exception:
                pass
    except Exception:
        pass
    
    # 自動預填/補齊權重配置 (避免既有資料庫因新增/改名因子而無法顯示)
    from database.models import ScoringWeight
    session = Session()
    try:
        need_seed = session.query(ScoringWeight).count() == 0
        if not need_seed:
            jt = session.query(ScoringWeight).filter_by(factor_name="jockey_trainer_bond").first()
            ds = session.query(ScoringWeight).filter_by(factor_name="draw_stats").first()
            obsolete = session.query(ScoringWeight).filter_by(factor_name="jockey_horse_bond").first()
            legacy = session.query(ScoringWeight).filter_by(factor_name="trainer_horse_bond").first()
            if obsolete or legacy:
                need_seed = True
            elif not jt or not ds:
                need_seed = True
            else:
                if (jt.description or "") != "騎師＋練馬師合作 (綜合)":
                    need_seed = True
                if (ds.description or "") != "檔位偏差 (官方 Draw Statistics)":
                    need_seed = True

        if need_seed:
            from scripts.init_db import populate_default_weights
            populate_default_weights()
    except Exception as e:
        print(f"預填權重失敗: {e}")
    finally:
        session.close()

def get_session():
    """獲取資料庫 Session"""
    return Session()
