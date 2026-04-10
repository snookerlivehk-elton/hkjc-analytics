import re
from typing import Dict, Any

import pandas as pd
import requests


class SpeedProEnergyScraper:
    def __init__(self):
        # The frontend loads data from this JSON endpoint
        self.base_url = "https://consvc.hkjc.com/-/media/Sites/JCRW/SpeedPro/current/sg_race_{}"

    def scrape(self, race_no: int) -> Dict[int, Dict[str, Any]]:
        url = self.base_url.format(race_no)
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        # If the race does not exist or API fails
        if r.status_code != 200:
            return {}

        try:
            # The API returns utf-8-sig (with BOM)
            data = r.json()
        except Exception:
            try:
                import json
                data = json.loads(r.content.decode('utf-8-sig'))
            except Exception:
                return {}

        out: Dict[int, Dict[str, Any]] = {}
        
        # Ensure we have the required structure
        if "zh-hk" not in data or "SpeedPRO" not in data["zh-hk"]:
            return out
            
        runners = data["zh-hk"]["SpeedPRO"]
        
        for runner in runners:
            try:
                horse_no = int(runner.get("runnernumber", 0))
            except (ValueError, TypeError):
                continue
                
            if not horse_no:
                continue
                
            # Parse Energy Required (A)
            energy_req_str = str(runner.get("energyrequired", "")).strip()
            energy_required = float(energy_req_str) if energy_req_str else None
            
            # Parse SpeedPRO Energy (B)
            energy_assess_str = str(runner.get("speedproenergy", "")).strip()
            energy_assess = float(energy_assess_str) if energy_assess_str else None
            
            # Parse Fitness Rating (狀態評級)
            # The API returns "0" for 👎, "1" for 1👍, "2" for 2👍, "3" for 3👍
            fitness_str = str(runner.get("fitnessrating", "")).strip()
            status_rating = int(fitness_str) if fitness_str.isdigit() else None
            
            # Calculate Difference (B) - (A)
            diff = None
            if energy_required is not None and energy_assess is not None:
                diff = float(energy_assess) - float(energy_required)
                
            out[horse_no] = {
                "energy_required": energy_required,
                "status_rating": status_rating,
                "energy_assess": energy_assess,
                "energy_diff": diff,
            }

        return out

