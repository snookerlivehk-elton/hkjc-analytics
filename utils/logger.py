import logging
import os
from datetime import datetime

def setup_logger(name="hkjc_analytics"):
    """配置專案全域 Logger"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 建立日誌目錄
    os.makedirs("logs", exist_ok=True)
    
    # 檔案 Handler
    log_filename = f"logs/app_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 終端機 Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 格式設定
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 加入 Handler
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# 全域預設 Logger
logger = setup_logger()
