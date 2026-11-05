"""Query the Spark-produced Parquet gold layer using DuckDB."""
import argparse
from pathlib import Path


def report(path: Path):
    import duckdb
    if not path.exists():
        raise FileNotFoundError(f'Gold layer not found: {path}. Run pipeline first.')
    con = duckdb.connect(database=':memory:')
    try:
        sql = """SELECT region, SUM(completed_orders) AS completed_orders,
                        ROUND(SUM(gross_revenue), 2) AS gross_revenue
                 FROM read_parquet(?) GROUP BY region ORDER BY gross_revenue DESC"""
        result = con.execute(sql, [str(path / '*.parquet')]).fetchall()
        print('region | completed_orders | gross_revenue')
        for row in result:
            print(' | '.join(str(v) for v in row))
        return result
    finally:
        con.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--gold', type=Path, default=Path('data/processed/gold_daily_sales'))
    args = p.parse_args()
    report(args.gold)
