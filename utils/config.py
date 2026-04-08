import os
import yaml
from typing import Dict, Any

class Config:
    """設定檔管理器 (支援 YAML)"""
    def __init__(self, config_path="config/settings.yaml"):
        self.config_path = config_path
        self.settings: Dict[str, Any] = {}
        self.load_config()

    def load_config(self):
        """從 YAML 載入設定，若無則使用預設值"""
        if os.path.exists(self.config_path):
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.settings = yaml.safe_load(f) or {}
        else:
            # 預設設定
            self.settings = {
                "scraping": {
                    "headless": True,
                    "timeout": 30000,
                    "rate_limit_delay": 2.0,
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                },
                "database": {
                    "url": "sqlite:///./data/hkjc_racing.db"
                },
                "scoring": {
                    "relative_percentile": True,
                    "score_range": [0, 10]
                }
            }
            # 確保 config 目錄存在並保存預設檔案
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.dump(self.settings, f, allow_unicode=True)

    def get(self, key: str, default: Any = None) -> Any:
        """獲取嵌套鍵值 (e.g. 'scraping.timeout')"""
        keys = key.split(".")
        value = self.settings
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

# 全域預設 Config
config = Config()
