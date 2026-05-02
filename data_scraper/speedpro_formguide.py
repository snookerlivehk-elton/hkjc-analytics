import json
import requests
from typing import Dict, Any

class SpeedProFormGuideScraper:
    def __init__(self):
        # The frontend loads FormGuide data from this JSON endpoint
        self.base_url = "https://consvc.hkjc.com/-/media/Sites/JCRW/SpeedPro/current/fg_race_{}"

    def scrape(self, race_no: int) -> Dict[int, Dict[str, Any]]:
        url = self.base_url.format(race_no)
        r = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://racing.hkjc.com/"
            },
            timeout=30,
        )
        
        if r.status_code != 200:
            return {}

        try:
            r.encoding = 'utf-8-sig'
            data = r.json()
        except Exception:
            try:
                data = json.loads(r.content.decode('utf-8-sig'))
            except Exception:
                return {}

        out: Dict[int, Dict[str, Any]] = {}
        
        if not isinstance(data, dict) or "SpeedPRO" not in data:
            return out
            
        runners = data["SpeedPRO"]
        if not isinstance(runners, list):
            return out
            
        for runner in runners:
            try:
                horse_no = int(runner.get("runnerno", 0))
            except (ValueError, TypeError):
                continue
                
            if not horse_no:
                continue
                
            records = runner.get("runnerrecords", [])
            parsed_records = []
            
            if isinstance(records, list):
                # Keep up to 6 recent runs to provide good context
                for rec in records[:6]:
                    if not isinstance(rec, dict):
                        continue
                    parsed_records.append({
                        "racedate": str(rec.get("racedate", "")).strip(),
                        "dist": str(rec.get("dist", "")).strip(),
                        "going": str(rec.get("going_chi", "")).strip(),
                        "fp": str(rec.get("fp", "")).strip(),
                        "pace": str(rec.get("pace_chi", "")).strip(),
                        "wide": str(rec.get("wide", "")).strip(),
                        "comments": str(rec.get("comments_chi", "")).strip(),
                        "incident": str(rec.get("incident_chi", "")).strip(),
                        "health": str(rec.get("healthissue_chi", "")).strip()
                    })
                    
            out[horse_no] = {
                "horse_name": str(runner.get("horse_chi", "")).strip(),
                "history": parsed_records
            }

        return out
