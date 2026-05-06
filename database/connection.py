import os
import re
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker, scoped_session
from database.models import Base

# 預設使用 SQLite，未來可改為 PostgreSQL 連線字串
_RAW_DATABASE_URL = os.getenv("DATABASE_URL")
_DEFAULT_SQLITE_URL = "sqlite:///./data/hkjc_racing.db"

if (not _RAW_DATABASE_URL) and (os.getenv("RAILWAY_PROJECT_ID") or os.getenv("RAILWAY_SERVICE_ID")):
    allow_sqlite = str(os.getenv("ALLOW_SQLITE") or "").strip().lower() in ("1", "true", "yes")
    if not allow_sqlite:
        raise RuntimeError(
            "未設定 DATABASE_URL。Railway 請把 service 連接到 Postgres（或在 Variables 設定 DATABASE_URL）。"
        )

DATABASE_URL = _RAW_DATABASE_URL or _DEFAULT_SQLITE_URL

# 修正 Railway 的 postgres:// 網址為 postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL.startswith("sqlite:///") and (":memory:" not in DATABASE_URL):
    p = DATABASE_URL.replace("sqlite:///", "", 1)
    p = re.sub(r"[?#].*$", "", p)
    try:
        fp = Path(p)
        if not fp.is_absolute():
            fp = (Path.cwd() / fp).resolve()
        fp.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

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
        try:
            if "postgresql" in DATABASE_URL:
                cols = {c["name"]: c for c in inspector.get_columns("system_configs")}
                key_col = cols.get("key")
                val_col = cols.get("value")
                with engine.begin() as conn:
                    if key_col is not None:
                        kt = key_col.get("type")
                        if getattr(kt, "length", None) is not None and int(getattr(kt, "length") or 0) < 120:
                            conn.execute(text("ALTER TABLE system_configs ALTER COLUMN key TYPE VARCHAR(255)"))

                    if val_col is not None:
                        vt = val_col.get("type")
                        if getattr(vt, "length", None) is not None and int(getattr(vt, "length") or 0) > 0:
                            conn.execute(
                                text(
                                    """
                                    ALTER TABLE system_configs
                                    ALTER COLUMN value TYPE JSONB
                                    USING (
                                      CASE
                                        WHEN value IS NULL OR btrim(value) = '' THEN '{}'::jsonb
                                        WHEN value ~ '^\\s*[\\{\\[]' THEN value::jsonb
                                        ELSE to_jsonb(value)
                                      END
                                    )
                                    """
                                )
                            )
        except Exception:
            pass

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
    
    try:
        inspector = inspect(engine)
        cols = {c["name"] for c in inspector.get_columns("races")}
        if "surface" not in cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE races ADD COLUMN surface VARCHAR(20)"))
            try:
                with engine.begin() as conn:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_races_surface ON races (surface)"))
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
        else:
            disabled = ("gear_change", "going_specialty", "morning_trial_perf", "odds_movement", "pace_analysis", "vet_rest_days")
            session.query(ScoringWeight).filter(ScoringWeight.factor_name.in_(disabled)).update(
                {ScoringWeight.is_active: False, ScoringWeight.weight: 0.0},
                synchronize_session=False,
            )
            sp = session.query(ScoringWeight).filter_by(factor_name="speedpro_energy").first()
            if sp and (sp.is_active is False) and (float(sp.weight or 0.0) == 0.0):
                sp.is_active = True
                sp.weight = 1.2
            session.commit()
    except Exception as e:
        print(f"預填權重失敗: {e}")
    finally:
        session.close()

    try:
        from database.models import Race
        import re

        session2 = Session()
        try:
            q = session2.query(Race)
            changed = 0
            for r in q.all():
                tt = str(getattr(r, "track_type", "") or "")
                go = str(getattr(r, "going", "") or "")

                if not (getattr(r, "surface", None) and str(getattr(r, "surface") or "").strip()):
                    if any(x in tt for x in ["全天候", "ALL WEATHER", "A/W", "AW", "泥地"]):
                        r.surface = "泥地"
                        changed += 1
                    elif any(x in tt for x in ["草地", "TURF"]):
                        r.surface = "草地"
                        changed += 1
                    elif go in {"草地", "泥地"}:
                        r.surface = go
                        changed += 1

                ct0 = str(getattr(r, "course_type", "") or "").strip()
                if (not ct0) or ct0.upper() == "U":
                    if "草地" in tt:
                        m = re.search(r"\"([A-Z0-9\\+]+)\"", tt)
                        if m:
                            r.course_type = str(m.group(1))
                            changed += 1
                    elif any(x in tt for x in ["全天候", "ALL WEATHER", "A/W", "AW", "泥地"]):
                        r.course_type = "AWT"
                        changed += 1

            if changed:
                session2.commit()
        finally:
            session2.close()
    except Exception:
        pass

    try:
        from database.models import RaceDividend, RaceTrackCondition
        from scoring_engine.track_conditions import normalize_going

        session3 = Session()
        try:
            divs = session3.query(RaceDividend.race_id, RaceDividend.meta).all()
            changed = 0
            for rid, meta in divs:
                if not isinstance(meta, dict):
                    continue
                going_raw, going_code = normalize_going(str(meta.get("going") or ""))
                track_raw = str(meta.get("track") or "").strip()
                if not (going_raw or track_raw):
                    continue
                tc = session3.query(RaceTrackCondition).filter_by(race_id=int(rid)).first()
                if not tc:
                    tc = RaceTrackCondition(race_id=int(rid), source="HKJC_LOCALRESULTS")
                    session3.add(tc)
                    changed += 1
                if going_raw and (not str(getattr(tc, "going_raw", "") or "").strip()):
                    tc.going_raw = going_raw
                    tc.going_code = going_code or going_raw
                    changed += 1
                if track_raw and (not str(getattr(tc, "track_raw", "") or "").strip()):
                    tc.track_raw = track_raw
                    changed += 1
            if changed:
                session3.commit()
        finally:
            session3.close()
    except Exception:
        pass

def get_session():
    """獲取資料庫 Session"""
    return Session()
