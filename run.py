"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

DBT_DIR = Path(__file__).resolve().parent / "dbt"

# Step 1 inputs. Only the pattern is fixed: whatever price files exist are loaded.
APP_TABLES = ("orders", "order_items", "products", "stores")
SUPPLIER_GLOB = "prices_*.csv"
# Both names the supplier has used for the cost column, so staging can always reference both.
COST_COLUMNS = ("unit_cost", "cost_sar")

# Step 5 outputs: Iceberg table -> (dbt relation in DuckDB, key columns used to sort before comparing).
PUBLISH = {
    "silver.product_cost_history": ("silver.product_cost_history", ["sku", "valid_from"]),
    "gold.weekly_margin": ("gold.weekly_margin", ["iso_week", "city", "category"]),
    "quality.price_quarantine": ("quality.price_quarantine", ["source_file", "source_row"]),
}


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Running this twice must not duplicate anything.
    """
    app = {name: _read_sqlite_table(data_dir / "qafila_oltp.sqlite", name) for name in APP_TABLES}
    prices = _read_supplier_files(data_dir / "suppliers")

    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        # CREATE OR REPLACE: every run rebuilds raw from the files, so a re-run never duplicates rows.
        for name, table in {**app, "supplier_prices": prices}.items():
            con.register("incoming", table)
            con.sql(f"CREATE OR REPLACE TABLE raw.{name} AS SELECT * FROM incoming")
            con.unregister("incoming")
            print(f"raw.{name}: {table.num_rows:,} rows")


def _read_sqlite_table(path: Path, table: str) -> pa.Table:
    """One app table, read with Python's sqlite3 (DuckDB's sqlite extension would need a download)."""
    if not path.exists():
        raise SystemExit(f"missing app database: {path}")
    with closing(sqlite3.connect(path)) as con:
        cur = con.execute(f'SELECT * FROM "{table}"')
        names = [d[0] for d in cur.description]
        rows = cur.fetchall()
    table = pa.table({n: pa.array([r[i] for r in rows]) for i, n in enumerate(names)})
    # A column that is NULL in every row has no type: land it as text.
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
    return table


def _read_supplier_files(folder: Path) -> pa.Table:
    """Every supplier price file, every value kept as the original text.

    The files change shape (renamed and added columns), so columns are united by name. `utf-8-sig`
    removes a byte-order mark if there is one. Each row keeps its file name and its row number.
    """
    files = sorted(folder.glob(SUPPLIER_GLOB))
    if not files:
        raise SystemExit(f"no supplier files matching {folder / SUPPLIER_GLOB}")
    rows: list[dict[str, str | int]] = []
    columns: list[str] = []
    for f in files:
        with f.open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            header = [h.strip() for h in (reader.fieldnames or [])]
            columns += [h for h in header if h not in columns]
            for i, raw in enumerate(reader, start=1):
                row: dict[str, str | int] = {k.strip(): v for k, v in raw.items() if k is not None}
                rows.append({**row, "source_file": f.name, "source_row": i})
    columns += [c for c in COST_COLUMNS if c not in columns]  # staging references both names
    schema = pa.schema([(c, pa.string()) for c in columns] + [("source_file", pa.string()), ("source_row", pa.int32())])
    return pa.Table.from_pylist(rows, schema=schema)


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg (see the brief for the exact columns).

        silver.product_cost_history   gold.weekly_margin   quality.price_quarantine

    Update existing tables in place, never drop and recreate them: the table's snapshots are
    how we see what a number was before the supplier's correction arrived.
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db), read_only=True) as con:
        new_data = {name: con.sql(f"SELECT * FROM {rel} ORDER BY ALL").to_arrow_table()
                    for name, (rel, _) in PUBLISH.items()}

    for name, data in new_data.items():
        keys = PUBLISH[name][1]
        catalog.create_namespace_if_not_exists(name.split(".")[0])
        try:
            table = catalog.load_table(name)
        except NoSuchTableError:
            table = catalog.create_table(name, schema=data.schema)
            table.append(data)
            print(f"{name}: created, {data.num_rows:,} rows")
            continue

        current = table.scan().to_arrow()
        if _same_rows(current, data, keys):
            # Idempotence: same data -> no write, no new snapshot.
            print(f"{name}: unchanged, {data.num_rows:,} rows")
            continue
        # The data changed (e.g. a correction file arrived): overwrite in place. Iceberg keeps the
        # previous snapshot, so the old numbers stay readable with time travel.
        with table.update_schema() as update:
            update.union_by_name(data.schema)
        table.overwrite(data)
        print(f"{name}: overwritten, {current.num_rows:,} -> {data.num_rows:,} rows")


def _same_rows(current: pa.Table, new: pa.Table, keys: list[str]) -> bool:
    """True when both tables hold the same rows in the published columns, regardless of row order."""
    if current.num_rows != new.num_rows or set(new.column_names) - set(current.column_names):
        return False
    order = [(k, "ascending") for k in keys]
    a = current.select(new.column_names).sort_by(order).cast(new.schema, safe=False)
    b = new.sort_by(order)
    return a.equals(b)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
