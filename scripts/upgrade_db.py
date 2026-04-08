import os
import psycopg2
import sys
from urllib.parse import urlparse

def upgrade_db():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("沒有設定 DATABASE_URL，跳過 PostgreSQL 升級")
        return

    try:
        # Railway PostgreSQL 連線
        result = urlparse(db_url)
        conn = psycopg2.connect(
            dbname=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port
        )
        cur = conn.cursor()
        
        # 檢查欄位是否存在
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='scoring_factors' AND column_name='raw_data_display';
        """)
        
        if not cur.fetchone():
            print("正在新增 raw_data_display 欄位到 scoring_factors...")
            cur.execute("ALTER TABLE scoring_factors ADD COLUMN raw_data_display VARCHAR(255);")
            conn.commit()
            print("資料庫升級成功！")
        else:
            print("欄位 raw_data_display 已存在，無需升級。")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"資料庫升級失敗: {e}")

if __name__ == "__main__":
    upgrade_db()
