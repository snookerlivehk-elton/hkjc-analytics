from sqlalchemy.orm import Session
from database.models import Horse, Jockey, Trainer, Race, RaceEntry, RaceResult, OddsHistory, ScoringWeight
from datetime import datetime
from typing import Dict, Any, List, Optional
from utils.logger import logger

class RacingRepository:
    """資料庫操作庫，封裝常用的 CRUD 邏輯"""

    def __init__(self, session: Session):
        self.session = session

    def get_or_create_horse(self, code: str, name_ch: str) -> Horse:
        horse = self.session.query(Horse).filter_by(code=code).first()
        if not horse:
            horse = Horse(code=code, name_ch=name_ch)
            self.session.add(horse)
            self.session.flush()
        return horse

    def get_or_create_jockey(self, name_ch: str) -> Jockey:
        # 這裡假設 name_ch 唯一，實際可能需要 code
        jockey = self.session.query(Jockey).filter_by(name_ch=name_ch).first()
        if not jockey:
            jockey = Jockey(code=name_ch, name_ch=name_ch)
            self.session.add(jockey)
            self.session.flush()
        return jockey

    def get_or_create_trainer(self, name_ch: str) -> Trainer:
        trainer = self.session.query(Trainer).filter_by(name_ch=name_ch).first()
        if not trainer:
            trainer = Trainer(code=name_ch, name_ch=name_ch)
            self.session.add(trainer)
            self.session.flush()
        return trainer

    def create_race(self, race_date: datetime, venue: str, race_no: int) -> Race:
        race_id = f"{race_date.strftime('%Y%m%d')}-{race_no}"
        race = self.session.query(Race).filter_by(race_id=race_id).first()
        if not race:
            race = Race(race_date=race_date, venue=venue, race_no=race_no, race_id=race_id)
            self.session.add(race)
            self.session.flush()
        return race

    def update_odds(self, entry_id: int, win_odds: float, place_odds: float, odds_type: str = "Live"):
        odds = OddsHistory(
            entry_id=entry_id,
            win_odds=win_odds,
            place_odds=place_odds,
            odds_type=odds_type,
            captured_at=datetime.now()
        )
        self.session.add(odds)
        self.session.commit()

    def get_active_weights(self) -> Dict[str, float]:
        weights = self.session.query(ScoringWeight).filter_by(is_active=True).all()
        return {w.factor_name: w.weight for w in weights}
