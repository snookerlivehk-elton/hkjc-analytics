import argparse
import sys
from pathlib import Path
from sqlalchemy import inspect, text

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import engine, init_db, DATABASE_URL


def _table_exists(inspector, name: str) -> bool:
    try:
        return bool(inspector.has_table(str(name)))
    except Exception:
        try:
            return str(name) in set(inspector.get_table_names())
        except Exception:
            return False


def _count(conn, sql: str, params=None) -> int:
    try:
        v = conn.execute(text(sql), params or {}).scalar()
        return int(v or 0)
    except Exception:
        return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", action="store_true")
    args = p.parse_args()

    init_db()
    is_pg = "postgresql" in str(DATABASE_URL or "").lower()

    with engine.begin() as conn:
        inspector = inspect(conn)

        n_cfg = 0
        n_sd = 0
        n_tbl = 0

        if _table_exists(inspector, "system_configs"):
            n_cfg = _count(conn, "SELECT COUNT(*) FROM system_configs WHERE key LIKE :p", {"p": "race_corunning:%"})

        if _table_exists(inspector, "search_documents"):
            n_sd = _count(conn, "SELECT COUNT(*) FROM search_documents WHERE doc_type = :t", {"t": "corunning"})

        if _table_exists(inspector, "race_corunning"):
            n_tbl = _count(conn, "SELECT COUNT(*) FROM race_corunning")

        print("purge_corunning_data")
        print(f"system_configs(race_corunning:%)={n_cfg}")
        print(f"search_documents(doc_type='corunning')={n_sd}")
        print(f"race_corunning(rows)={n_tbl}")

        if not args.run:
            print("dry_run=true (use --run to execute)")
            return

        if _table_exists(inspector, "search_documents"):
            conn.execute(text("DELETE FROM search_documents WHERE doc_type = :t"), {"t": "corunning"})
        if _table_exists(inspector, "system_configs"):
            conn.execute(text("DELETE FROM system_configs WHERE key LIKE :p"), {"p": "race_corunning:%"})
        if _table_exists(inspector, "race_corunning"):
            if is_pg:
                conn.execute(text("DROP TABLE IF EXISTS race_corunning CASCADE"))
            else:
                conn.execute(text("DROP TABLE IF EXISTS race_corunning"))

        print("ok=true")


if __name__ == "__main__":
    main()

