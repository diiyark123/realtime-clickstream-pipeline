"""Query the Parquet data lake with DuckDB (no Spark needed).

Usage:  pip install duckdb && python scripts/inspect_lake.py [path-to-lake]
"""

import sys
from pathlib import Path

import duckdb

lake = Path(sys.argv[1] if len(sys.argv) > 1 else "data/lake/events")
files = list(lake.rglob("*.parquet"))
if not files:
    sys.exit(f"No Parquet files under {lake} yet. Is the Spark job running?")

src = f"read_parquet('{lake.as_posix()}/**/*.parquet', hive_partitioning = true)"
con = duckdb.connect()

print(f"Parquet files: {len(files)}\n")
queries = {
    "Events by type": f"SELECT event_type, COUNT(*) AS events FROM {src} GROUP BY 1 ORDER BY 2 DESC",
    "Events per day (partition)": f"SELECT event_date, COUNT(*) AS events FROM {src} GROUP BY 1 ORDER BY 1",
    "Duplicate event_ids (should be 0)":
        f"SELECT COUNT(*) AS duplicates FROM (SELECT event_id FROM {src} GROUP BY 1 HAVING COUNT(*) > 1)",
    "Revenue by device": f"""SELECT device, ROUND(SUM(price * quantity), 2) AS revenue
                             FROM {src} WHERE event_type = 'purchase' GROUP BY 1 ORDER BY 2 DESC""",
}
for title, q in queries.items():
    print(f"== {title}")
    print(con.sql(q))
