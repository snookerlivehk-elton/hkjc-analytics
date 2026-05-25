import os
import re
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker
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

engine_kwargs = {"echo": False, "connect_args": connect_args}
if "postgresql" in DATABASE_URL:
    engine_kwargs.update(
        {
            "pool_pre_ping": True,
            "pool_recycle": int(os.getenv("DB_POOL_RECYCLE_SEC") or 300),
            "pool_size": int(os.getenv("DB_POOL_SIZE") or 5),
            "max_overflow": int(os.getenv("DB_POOL_MAX_OVERFLOW") or 5),
            "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT_SEC") or 30),
        }
    )

engine = create_engine(DATABASE_URL, **engine_kwargs)

# 建立 Session 工廠
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

def init_db():
    """初始化資料庫表結構並預填權重"""
    Base.metadata.create_all(engine)

    try:
        with engine.begin() as conn:
            if "postgresql" in DATABASE_URL:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_system_configs_key_pattern ON system_configs (key text_pattern_ops)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_entries_race_id ON race_entries (race_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_entries_race_id_horse_no ON race_entries (race_id, horse_no)"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_race_entries_race_id_total_score_horse_no ON race_entries (race_id, total_score DESC, horse_no ASC)"
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_entries_horse_id_race_id ON race_entries (horse_id, race_id)"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_race_results_entry_id ON race_results (entry_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_scoring_factors_entry_id ON scoring_factors (entry_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_scoring_factors_entry_factor ON scoring_factors (entry_id, factor_name)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_odds_history_entry_id ON odds_history (entry_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_odds_history_type_entry_cap ON odds_history (odds_type, entry_id, captured_at DESC)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_pool_snapshots_race_id ON race_pool_snapshots (race_id)"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_race_pool_snapshots_day_no_type ON race_pool_snapshots (race_date_day, venue, race_no, snapshot_type)"
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_pace_snapshots_race_id ON race_pace_snapshots (race_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_pace_snapshots_day_no ON race_pace_snapshots (race_date_day, venue, race_no)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_pace_forecasts_race_id ON race_pace_forecasts (race_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_race_pace_forecasts_day_no ON race_pace_forecasts (race_date_day, venue, race_no)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_prediction_top5_race_date_no ON prediction_top5 (race_date, race_no)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_prediction_top5_type_key_date ON prediction_top5 (predictor_type, predictor_key, race_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_prediction_top5_type_email_date ON prediction_top5 (predictor_type, member_email, race_date)"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_prediction_top5_preset_member_key_date_no ON prediction_top5 (predictor_type, member_email, predictor_key, race_date, race_no)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_prediction_top5_factor_key_date_no ON prediction_top5 (predictor_type, predictor_key, race_date, race_no)"
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_system_configs_updated_at ON system_configs (updated_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_system_configs_job_prefix_updated_at ON system_configs (key, updated_at DESC)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_raw_snapshots_entity ON raw_snapshots (entity_type, entity_key)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_search_documents_entity ON search_documents (entity_type, entity_key)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_search_documents_race_day ON search_documents (race_date_day, race_no)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_search_documents_doc_type ON search_documents (doc_type, updated_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_search_documents_day_updated ON search_documents (race_date_day DESC, updated_at DESC)"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_search_documents_doc_day_updated ON search_documents (doc_type, race_date_day DESC, updated_at DESC)"
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_races_race_date_race_no ON races (race_date, race_no)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_entry_facts_day_draw ON entry_facts (race_date_day, draw)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_entry_facts_day_jockey ON entry_facts (race_date_day, jockey_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_entry_facts_day_trainer ON entry_facts (race_date_day, trainer_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_draw_stats_daily_day_draw ON draw_stats_daily (race_date_day, draw)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_draw_stats_daily_day_updated ON draw_stats_daily (race_date_day DESC, updated_at DESC)"))
            if "postgresql" in DATABASE_URL:
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_search_documents_search_trgm ON search_documents USING GIN (search_text gin_trgm_ops)"))
    except Exception:
        pass

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
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_horse_histories_horse_id_race_date ON horse_histories (horse_id, race_date DESC)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_horse_histories_dedupe_key ON horse_histories (horse_id, race_date, venue, distance, race_class)"))
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
        if "post_time_hk" not in cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE races ADD COLUMN post_time_hk VARCHAR(5)"))
    except Exception:
        pass

    try:
        inspector = inspect(engine)
        cols = {c["name"] for c in inspector.get_columns("entry_facts")}
        to_add = []
        if "runpos_early" not in cols:
            to_add.append(("runpos_early", "INTEGER"))
        if "runstyle_bucket" not in cols:
            to_add.append(("runstyle_bucket", "VARCHAR(20)"))
        if "pace_delta_sec" not in cols:
            to_add.append(("pace_delta_sec", "DOUBLE PRECISION"))
        if "pace_bucket" not in cols:
            to_add.append(("pace_bucket", "VARCHAR(20)"))
        if to_add:
            with engine.begin() as conn:
                for name, typ in to_add:
                    conn.execute(text(f"ALTER TABLE entry_facts ADD COLUMN {name} {typ}"))
            try:
                with engine.begin() as conn:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_entry_facts_day_runstyle ON entry_facts (race_date_day, runstyle_bucket)"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_entry_facts_day_pace_bucket ON entry_facts (race_date_day, pace_bucket)"))
            except Exception:
                pass
    except Exception:
        pass

    # 自動預填/補齊權重配置 (避免既有資料庫因新增/改名因子而無法顯示)
    from database.models import ScoringWeight
    session = SessionLocal()
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
            disabled = ("gear_change", "going_specialty", "morning_trial_perf", "odds_movement", "pace_analysis", "recent_running_style", "vet_rest_days")
            session.query(ScoringWeight).filter(ScoringWeight.factor_name.in_(disabled)).update(
                {ScoringWeight.is_active: False, ScoringWeight.weight: 0.0},
                synchronize_session=False,
            )
            sp = session.query(ScoringWeight).filter_by(factor_name="speedpro_energy").first()
            if sp and (sp.is_active is False) and (float(sp.weight or 0.0) == 0.0):
                sp.is_active = True
                sp.weight = 1.2

            upserts = [
                ("recent_running_style", "近期跑法（近6仗沿途走位）", 0.0, False),
                ("style_trkprof_edge", "跑法適配分（跑道×場地狀態｜勝出/入圍）", 0.0, True),
            ]
            for fn, desc, w0, active0 in upserts:
                row = session.query(ScoringWeight).filter_by(factor_name=str(fn)).first()
                if not row:
                    row = ScoringWeight(
                        factor_name=str(fn),
                        description=str(desc),
                        weight=float(w0),
                        is_active=bool(active0),
                    )
                    session.add(row)
                else:
                    if str(desc or "").strip() and (str(row.description or "").strip() != str(desc or "").strip()):
                        row.description = str(desc)
                    if row.is_active != bool(active0):
                        row.is_active = bool(active0)
            session.commit()
    except Exception as e:
        print(f"預填權重失敗: {e}")
    finally:
        session.close()

    try:
        from database.models import Race
        import re

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE races
                        SET surface='泥地'
                        WHERE (surface IS NULL OR surface='')
                          AND (
                            track_type LIKE '%全天候%'
                            OR UPPER(track_type) LIKE '%ALL WEATHER%'
                            OR UPPER(track_type) LIKE '%A/W%'
                            OR UPPER(track_type) LIKE '%AW%'
                            OR track_type LIKE '%泥地%'
                          )
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        UPDATE races
                        SET surface='草地'
                        WHERE (surface IS NULL OR surface='')
                          AND (
                            track_type LIKE '%草地%'
                            OR UPPER(track_type) LIKE '%TURF%'
                          )
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        UPDATE races
                        SET surface=going
                        WHERE (surface IS NULL OR surface='')
                          AND (going IN ('草地','泥地'))
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        UPDATE races
                        SET course_type='AWT'
                        WHERE (course_type IS NULL OR course_type='' OR course_type='U' OR course_type='u')
                          AND (
                            track_type LIKE '%全天候%'
                            OR UPPER(track_type) LIKE '%ALL WEATHER%'
                            OR UPPER(track_type) LIKE '%A/W%'
                            OR UPPER(track_type) LIKE '%AW%'
                            OR track_type LIKE '%泥地%'
                          )
                        """
                    )
                )
        except Exception:
            pass

        session2 = SessionLocal()
        try:
            q = (
                session2.query(Race)
                .filter((Race.course_type == None) | (Race.course_type == "") | (Race.course_type == "U") | (Race.course_type == "u"))
                .filter(Race.track_type != None)
                .filter(Race.track_type.like("%\"%"))
                .limit(5000)
                .all()
            )
            changed = 0
            for r in q:
                tt = str(getattr(r, "track_type", "") or "")
                m = re.search(r"\"([A-Z0-9\\+]+)\"", tt)
                if m:
                    r.course_type = str(m.group(1))
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

        session3 = SessionLocal()
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
    return SessionLocal()
