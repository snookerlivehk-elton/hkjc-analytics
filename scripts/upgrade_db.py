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
        
        # 1. 檢查 scoring_factors 是否有 raw_data_display 欄位
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='scoring_factors' AND column_name='raw_data_display';
        """)
        if not cur.fetchone():
            print("正在新增 raw_data_display 欄位...")
            cur.execute("ALTER TABLE scoring_factors ADD COLUMN raw_data_display VARCHAR(500);")
            conn.commit()
            print("raw_data_display 欄位新增成功！")
        else:
            print("欄位 raw_data_display 已存在，無需升級。")
        
        # 2. 檢查 horse_histories 是否有 trainer_name 欄位
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='horse_histories' AND column_name='trainer_name';
        """)
        if not cur.fetchone():
            print("正在新增 trainer_name 欄位...")
            cur.execute("ALTER TABLE horse_histories ADD COLUMN trainer_name VARCHAR(50);")
            conn.commit()
            print("trainer_name 欄位新增成功！")
        else:
            print("欄位 trainer_name 已存在，無需升級。")
            
        # 3. 檢查 system_configs 資料表是否存在，不存在則建立
        cur.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname='public' AND tablename='system_configs';
        """)
        
        if not cur.fetchone():
            print("正在建立 system_configs 資料表...")
            cur.execute("""
                CREATE TABLE system_configs (
                    id SERIAL PRIMARY KEY,
                    key VARCHAR(50) UNIQUE NOT NULL,
                    value JSON NOT NULL,
                    description VARCHAR(200),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX ix_system_configs_key ON system_configs (key);
            """)
            conn.commit()
            print("資料庫升級成功！")
        else:
            print("資料表 system_configs 已存在，無需升級。")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"資料庫升級失敗: {e}")

if __name__ == "__main__":
    upgrade_db()
