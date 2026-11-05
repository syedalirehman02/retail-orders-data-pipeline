import csv
from pathlib import Path
from src.generate_data import generate


def test_generates_expected_rows_and_schema(tmp_path: Path):
    output = tmp_path / 'orders.csv'
    generate(output, rows=10)
    with output.open(newline='') as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 10
    assert set(rows[0]) == {'order_id', 'order_date', 'customer_id', 'region', 'amount', 'status'}
    assert len({row['order_id'] for row in rows}) == 10
    assert all(float(row['amount']) > 0 for row in rows)


def test_seed_is_deterministic(tmp_path: Path):
    a, b = tmp_path / 'a.csv', tmp_path / 'b.csv'
    generate(a, rows=20)
    generate(b, rows=20)
    assert a.read_bytes() == b.read_bytes()
