from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Text, JSON, UniqueConstraint
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
    race_class = Column(String(20))
    distance = Column(Integer)
    track_type = Column(String(20))  # Turf, All Weather
    course_type = Column(String(10))  # A, B, C, C+3 etc.
    going = Column(String(20))  # Good, Yielding, etc.
    prize_money = Column(Float)
    created_at = Column(DateTime, default=datetime.now)

    entries = relationship("RaceEntry", back_populates="race")
    
    __table_args__ = (UniqueConstraint('race_date', 'venue', 'race_no', name='_race_date_venue_no_uc'),)

class RaceEntry(Base):
    """馬匹出賽排位資料"""
    __tablename__ = 'race_entries'
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('races.id'), nullable=False)
    horse_id = Column(Integer, ForeignKey('horses.id'), nullable=False)
    jockey_id = Column(Integer, ForeignKey('jockeys.id'))
    trainer_id = Column(Integer, ForeignKey('trainers.id'))
    
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
    entry_id = Column(Integer, ForeignKey('race_entries.id'), unique=True)
    
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
    entry_id = Column(Integer, ForeignKey('race_entries.id'))
    
    factor_name = Column(String(50), nullable=False)  # 條件名稱 (e.g. jockey_trainer_bond)
    raw_value = Column(Float)  # 原始數據值
    raw_data_display = Column(String(255), nullable=True) # 透明化原始數據文字 (e.g., "同程勝率 45%")
    score = Column(Float)      # 0-10 分 (相對排名得分)
    weight_at_time = Column(Float) # 計算時使用的權重
    
    entry = relationship("RaceEntry", back_populates="scoring_factors")

class OddsHistory(Base):
    """賠率變化歷史"""
    __tablename__ = 'odds_history'
    id = Column(Integer, primary_key=True)
    entry_id = Column(Integer, ForeignKey('race_entries.id'))
    odds_type = Column(String(20)) # Early, Live
    win_odds = Column(Float)
    place_odds = Column(Float)
    captured_at = Column(DateTime, default=datetime.now)

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
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class HorseHistory(Base):
    """馬匹歷史往績 (簡化版，用於快速計分)"""
    __tablename__ = 'horse_histories'
    id = Column(Integer, primary_key=True)
    horse_id = Column(Integer, ForeignKey('horses.id'))
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
