"""Generate deterministic synthetic retail orders."""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path


def generate(output: Path, rows: int = 5000, seed: int = 42) -> None:
    rng = random.Random(seed)
    output.parent.mkdir(parents=True, exist_ok=True)
    regions = ['North', 'South', 'East', 'West']
    with output.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['order_id', 'order_date', 'customer_id', 'region', 'amount', 'status'])
        for index in range(rows):
            day = date(2026, 1, 1) + timedelta(days=rng.randrange(90))
            writer.writerow([f'ORD-{index+1:07d}', day.isoformat(), f'C-{rng.randrange(1, 1001):04d}',
                             rng.choice(regions), f'{rng.uniform(10, 500):.2f}',
                             rng.choices(['completed', 'refunded', 'cancelled'], [85, 5, 10])[0]])


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--output', type=Path, default=Path('data/raw/orders.csv'))
    p.add_argument('--rows', type=int, default=5000)
    args = p.parse_args()
    if args.rows < 1:
        p.error('--rows must be positive')
    generate(args.output, args.rows)
    print(f'Generated {args.rows} synthetic orders in {args.output}')
