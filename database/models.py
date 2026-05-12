from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Boolean, Text, JSON, UniqueConstraint
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()

class Horse(Base):
    """馬匹基本資料"""
    __tablename__ = 'horses'
    id = Column(Integer, primary_key=True)
    code = Column(String(10), unique=True, nullable=False, index=True)  # 馬匹編號 (e.g. H123)
    name_ch = Column(String(50), nullable=False)
    name_en = Column(String(100))
    sex = Column(String(10))
    origin = Column(String(20))
    color = Column(String(20))
    sire = Column(String(100))
    dam = Column(String(100))
    import_type = Column(String(10))  # PPG, ISG, etc.
    current_rating = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    entries = relationship("RaceEntry", back_populates="horse")

class Jockey(Base):
    """騎師基本資料"""
    __tablename__ = 'jockeys'
    id = Column(Integer, primary_key=True)
    code = Column(String(10), unique=True, nullable=False, index=True)
    name_ch = Column(String(50), nullable=False)
    name_en = Column(String(100))
    weight_allowance = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)

    entries = relationship("RaceEntry", back_populates="jockey")

class Trainer(Base):
    """練馬師基本資料"""
    __tablename__ = 'trainers'
    id = Column(Integer, primary_key=True)
    code = Column(String(10), unique=True, nullable=False, index=True)
    name_ch = Column(String(50), nullable=False)
    name_en = Column(String(100))
    created_at = Column(DateTime, default=datetime.now)

    entries = relationship("RaceEntry", back_populates="trainer")

class Race(Base):
    """賽事基本資料"""
    __tablename__ = 'races'
    id = Column(Integer, primary_key=True)
    race_date = Column(DateTime, nullable=False, index=True)
    venue = Column(String(10), nullable=False)  # ST, HV
    race_no = Column(Integer, nullable=False)
    race_id = Column(String(20), unique=True, index=True)  # 20240408-1 (日期+場次)
    post_time_hk = Column(String(5))  # HH:MM（以香港時間表示）
    race_class = Column(String(20))
    distance = Column(Integer)
    track_type = Column(String(20))  # Turf, All Weather
    course_type = Column(String(10))  # A, B, C, C+3 etc.
    surface = Column(String(20), index=True)  # 草地 / 泥地(全天候)
    going = Column(String(20))  # Good, Yielding, etc.
    prize_money = Column(Float)
    created_at = Column(DateTime, default=datetime.now)

    entries = relationship("RaceEntry", back_populates="race")
    
    __table_args__ = (UniqueConstraint('race_date', 'venue', 'race_no', name='_race_date_venue_no_uc'),)


class RaceTrackCondition(Base):
    __tablename__ = "race_track_conditions"
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey("races.id"), unique=True, index=True, nullable=False)
    source = Column(String(30), default="HKJC_LOCALRESULTS")
    going_raw = Column(String(50))
    going_code = Column(String(20), index=True)
    track_raw = Column(String(50))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    race = relationship("Race")

class RaceEntry(Base):
    """馬匹出賽排位資料"""
    __tablename__ = 'race_entries'
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('races.id'), nullable=False, index=True)
    horse_id = Column(Integer, ForeignKey('horses.id'), nullable=False, index=True)
    jockey_id = Column(Integer, ForeignKey('jockeys.id'), index=True)
    trainer_id = Column(Integer, ForeignKey('trainers.id'), index=True)
    
    horse_no = Column(Integer)
    draw = Column(Integer)
    declared_weight = Column(Integer)
    actual_weight = Column(Integer)
    rating = Column(Integer)
    gear = Column(String(50))  # 配備
    horse_status = Column(String(50)) # 狀態
    
    # 計分結果 (存儲最終得分與排名)
    total_score = Column(Float)
    rank_in_race = Column(Integer)
    win_probability = Column(Float)
    
    race = relationship("Race", back_populates="entries")
    horse = relationship("Horse", back_populates="entries")
    jockey = relationship("Jockey", back_populates="entries")
    trainer = relationship("Trainer", back_populates="entries")
    
    result = relationship("RaceResult", back_populates="entry", uselist=False)
    scoring_factors = relationship("ScoringFactor", back_populates="entry")

class RaceResult(Base):
    """賽事結果"""
    __tablename__ = 'race_results'
    id = Column(Integer, primary_key=True)
    entry_id = Column(Integer, ForeignKey('race_entries.id'), unique=True, index=True)
    
    rank = Column(Integer)
    finish_time = Column(String(20))
    finish_time_sec = Column(Float)
    win_odds = Column(Float)
    place_odds = Column(Float)
    margin = Column(String(20))  # 勝負距離
    sectional_times = Column(JSON) # [23.1, 22.5, ...]
    
    entry = relationship("RaceEntry", back_populates="result")

class ScoringFactor(Base):
    """獨立計分條件得分"""
    __tablename__ = 'scoring_factors'
    id = Column(Integer, primary_key=True)
    entry_id = Column(Integer, ForeignKey('race_entries.id'), index=True)
    
    factor_name = Column(String(50), nullable=False, index=True)  # 條件名稱 (e.g. jockey_trainer_bond)
    raw_value = Column(Float)  # 原始數據值
    raw_data_display = Column(String(255), nullable=True) # 透明化原始數據文字 (e.g., "同程勝率 45%")
    score = Column(Float)      # 0-10 分 (相對排名得分)
    weight_at_time = Column(Float) # 計算時使用的權重
    
    entry = relationship("RaceEntry", back_populates="scoring_factors")

class OddsHistory(Base):
    """賠率變化歷史"""
    __tablename__ = 'odds_history'
    id = Column(Integer, primary_key=True)
    entry_id = Column(Integer, ForeignKey('race_entries.id'), index=True)
    odds_type = Column(String(20)) # Early, Live
    win_odds = Column(Float)
    place_odds = Column(Float)
    captured_at = Column(DateTime, default=datetime.now)

class RacePoolSnapshot(Base):
    __tablename__ = "race_pool_snapshots"
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey("races.id"), index=True, nullable=False)
    race_date_day = Column(Date, index=True, nullable=False)
    venue = Column(String(10), index=True, nullable=False)
    race_no = Column(Integer, index=True, nullable=False)
    snapshot_type = Column(String(20), index=True, nullable=False)  # PRE_24H | PRE_0100 | PRE_30M | PRE_15M | PRE_10M | PRE_5M | LIVE
    source = Column(String(30), default="BET_WP", index=True)
    update_time_hk = Column(String(20))
    pools = Column(JSON)
    win_pool = Column(Integer)
    place_pool = Column(Integer)
    captured_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    race = relationship("Race")

    __table_args__ = (UniqueConstraint("race_id", "snapshot_type", "source", name="ux_race_pool_snapshots_race_type_source"),)


class RacePaceSnapshot(Base):
    __tablename__ = "race_pace_snapshots"
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey("races.id"), unique=True, index=True, nullable=False)
    race_date_day = Column(Date, index=True, nullable=False)
    venue = Column(String(10), index=True, nullable=False)
    race_no = Column(Integer, index=True, nullable=False)
    distance = Column(Integer)
    surface_code = Column(String(10), index=True)
    race_class = Column(String(20))

    k_segments = Column(Integer)
    actual_sec = Column(Float)
    ref_sec = Column(Float)
    delta_sec = Column(Float, index=True)
    pace_class = Column(String(20), index=True)  # very_fast|fast|moderate_fast|moderate|moderate_slow|slow|very_slow|unknown
    meta = Column(JSON)

    computed_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    race = relationship("Race")


class RacePaceForecastSnapshot(Base):
    __tablename__ = "race_pace_forecasts"
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey("races.id"), unique=True, index=True, nullable=False)
    race_date_day = Column(Date, index=True, nullable=False)
    venue = Column(String(10), index=True, nullable=False)
    race_no = Column(Integer, index=True, nullable=False)
    distance = Column(Integer)
    surface_code = Column(String(10), index=True)
    race_class = Column(String(20))

    sample_n = Column(Integer)
    field_size = Column(Integer)
    front_count = Column(Integer)
    leader_count = Column(Integer)
    front_sum = Column(Float)
    pace_class = Column(String(20), index=True)  # very_fast|fast|moderate_fast|moderate|moderate_slow|slow|very_slow|unknown
    confidence = Column(String(10), index=True)  # high|mid|low
    meta = Column(JSON)

    computed_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    race = relationship("Race")


class RaceDividend(Base):
    __tablename__ = 'race_dividends'
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('races.id'), unique=True, index=True, nullable=False)
    source = Column(String(50), default="HKJC")
    dividends = Column(JSON)
    meta = Column(JSON)
    scraped_at = Column(DateTime, default=datetime.now)

class RaceCoRunning(Base):
    __tablename__ = "race_corunning"
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey("races.id"), unique=True, index=True, nullable=False)
    race_date = Column(DateTime, index=True, nullable=False)
    race_no = Column(Integer, index=True, nullable=False)
    source = Column(String(50), default="HKJC")
    items = Column(JSON, nullable=False)  # {horse_no: {horse_name, commentary}}
    meta = Column(JSON)
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class RawSnapshot(Base):
    __tablename__ = "raw_snapshots"
    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False, index=True)
    entity_type = Column(String(30), nullable=False, index=True)  # race | race_day | horse | venue_day ...
    entity_key = Column(String(80), nullable=False, index=True)  # YYYY/MM/DD:ST:1 or YYYY/MM/DD:ST
    race_id = Column(Integer, ForeignKey("races.id"), index=True, nullable=True)
    payload = Column(JSON, nullable=False)
    meta = Column(JSON)
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (UniqueConstraint("source", "entity_type", "entity_key", name="ux_raw_snapshots_source_entity"),)


class SearchDocument(Base):
    __tablename__ = "search_documents"
    id = Column(Integer, primary_key=True)
    doc_type = Column(String(30), nullable=False, index=True)  # race_entry | ai_report | corunning | runpos | ...
    ref_key = Column(String(120), nullable=False, index=True)  # stable identifier (e.g. race_key:horse_no, SystemConfig.key)
    entity_type = Column(String(30), nullable=False, index=True)
    entity_key = Column(String(80), nullable=False, index=True)
    race_id = Column(Integer, ForeignKey("races.id"), index=True, nullable=True)
    race_date_day = Column(Date, index=True, nullable=True)
    race_no = Column(Integer, index=True, nullable=True)
    venue = Column(String(10), index=True)
    surface_code = Column(String(10), index=True)  # AW | TURF | U
    course_type = Column(String(10), index=True)
    going_code = Column(String(20), index=True)
    horse_name = Column(String(80), index=True)
    jockey_name = Column(String(80), index=True)
    trainer_name = Column(String(80), index=True)
    title = Column(String(200))
    search_text = Column(Text, nullable=False)
    payload_excerpt = Column(JSON)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (UniqueConstraint("doc_type", "ref_key", name="ux_search_documents_doc_ref"),)

class ScoringWeight(Base):
    """計分權重配置"""
    __tablename__ = 'scoring_weights'
    id = Column(Integer, primary_key=True)
    factor_name = Column(String(50), unique=True, nullable=False)
    weight = Column(Float, default=1.0)
    description = Column(String(200))
    is_active = Column(Boolean, default=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class Workout(Base):
    """晨操/試閘資料"""
    __tablename__ = 'workouts'
    id = Column(Integer, primary_key=True)
    horse_id = Column(Integer, ForeignKey('horses.id'))
    workout_date = Column(DateTime)
    workout_type = Column(String(20)) # Trial, Morning Track
    description = Column(Text)
    rating = Column(Integer) # 1-5 星級或分數

class VetReport(Base):
    """獸醫報告"""
    __tablename__ = 'vet_reports'
    id = Column(Integer, primary_key=True)
    horse_id = Column(Integer, ForeignKey('horses.id'))
    report_date = Column(DateTime)
    details = Column(Text)
    severity = Column(Integer) # 嚴重程度

class SystemConfig(Base):
    """系統設定與算法參數 (儲存可變參數如權重衰減)"""
    __tablename__ = 'system_configs'
    id = Column(Integer, primary_key=True)
    key = Column(String(50), unique=True, nullable=False, index=True)
    value = Column(JSON, nullable=False)
    description = Column(String(200))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)


class PredictionTop5(Base):
    __tablename__ = "prediction_top5"
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey("races.id"), index=True, nullable=False)
    race_date = Column(DateTime, index=True, nullable=False)
    race_no = Column(Integer, nullable=False)
    predictor_type = Column(String(20), nullable=False)  # factor | preset
    predictor_key = Column(String(100), nullable=False)  # factor_name | preset_name
    member_email = Column(String(120), nullable=True)
    top5 = Column(JSON, nullable=False)
    meta = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            "race_id",
            "predictor_type",
            "predictor_key",
            "member_email",
            name="uq_prediction_top5",
        ),
    )

    race = relationship("Race")

class HorseHistory(Base):
    """馬匹歷史往績 (簡化版，用於快速計分)"""
    __tablename__ = 'horse_histories'
    id = Column(Integer, primary_key=True)
    horse_id = Column(Integer, ForeignKey('horses.id'), index=True)
    race_date = Column(DateTime, index=True)
    venue = Column(String(20))
    surface = Column(String(20), index=True)  # 草地 / 泥地(全天候)
    race_class = Column(String(20))
    distance = Column(Integer)
    rank = Column(Integer)
    draw = Column(Integer)
    jockey_name = Column(String(50))
    trainer_name = Column(String(50))
    weight = Column(Integer)
    rating = Column(Integer)
    finish_time = Column(String(20))
    created_at = Column(DateTime, default=datetime.now)
    
    horse = relationship("Horse")


class EntryFact(Base):
    __tablename__ = "entry_facts"
    id = Column(Integer, primary_key=True)
    entry_id = Column(Integer, ForeignKey("race_entries.id"), unique=True, index=True, nullable=False)
    race_id = Column(Integer, ForeignKey("races.id"), index=True, nullable=False)
    race_date_day = Column(Date, index=True, nullable=False)
    venue = Column(String(10), index=True)
    race_no = Column(Integer, index=True)

    race_class = Column(String(20), index=True)
    distance = Column(Integer, index=True)
    surface = Column(String(20), index=True)
    course_type = Column(String(10), index=True)
    going_code = Column(String(20), index=True)

    horse_id = Column(Integer, ForeignKey("horses.id"), index=True)
    jockey_id = Column(Integer, ForeignKey("jockeys.id"), index=True)
    trainer_id = Column(Integer, ForeignKey("trainers.id"), index=True)

    draw = Column(Integer, index=True)
    rating = Column(Integer, index=True)

    runpos_early = Column(Integer)
    runstyle_bucket = Column(String(20), index=True)
    pace_delta_sec = Column(Float)
    pace_bucket = Column(String(20), index=True)

    rank = Column(Integer, index=True)
    is_win = Column(Boolean, index=True)
    is_place = Column(Boolean, index=True)

    sp_win_odds = Column(Float)
    odds_bucket_sp = Column(String(20), index=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class DrawStatsDaily(Base):
    __tablename__ = "draw_stats_daily"
    id = Column(Integer, primary_key=True)
    race_date_day = Column(Date, index=True, nullable=False)

    venue = Column(String(10), index=True)
    surface = Column(String(20), index=True)
    course_type = Column(String(10), index=True)
    going_code = Column(String(20), index=True)
    race_class = Column(String(20), index=True)
    distance = Column(Integer, index=True)

    draw = Column(Integer, index=True, nullable=False)
    odds_bucket_sp = Column(String(20), index=True, nullable=False)

    samples = Column(Integer, nullable=False, default=0)
    win_cnt = Column(Integer, nullable=False, default=0)
    place_cnt = Column(Integer, nullable=False, default=0)

    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint(
            "race_date_day",
            "venue",
            "surface",
            "course_type",
            "going_code",
            "race_class",
            "distance",
            "draw",
            "odds_bucket_sp",
            name="ux_draw_stats_daily_dims",
        ),
    )


class RaceDayWeather(Base):
    __tablename__ = "race_day_weather"
    id = Column(Integer, primary_key=True)
    race_date_day = Column(Date, index=True, nullable=False)
    venue = Column(String(10), index=True, nullable=False)

    updated_at_text = Column(String(40))
    temperature_c = Column(Float)
    humidity_pct = Column(Float)
    rain_total_mm = Column(Float)
    rain_10min_mm = Column(Float)
    soil_moisture_pct = Column(Float)
    wind_direction = Column(String(20))
    wind_speed_kmh_avg = Column(Float)
    wind_speed_kmh_max = Column(Float)
    raw = Column(JSON)

    fetched_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (UniqueConstraint("race_date_day", "venue", name="ux_race_day_weather_day_venue"),)
