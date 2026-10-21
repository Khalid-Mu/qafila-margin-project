# Qafila gross margin

Submitted by: Khalid Almuhanna — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
```

## Design

`data/*.sqlite + suppliers/*.csv` → **raw** (Python + DuckDB, `run.py: ingest`) → **staging** (dbt views) → **silver / quality** (dbt tables) → **gold** (dbt table) → **Iceberg** (PyIceberg, `run.py: publish`)

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `raw.orders`, `raw.order_items`, `raw.products`, `raw.stores` | raw | source row, as in the app database | table (`CREATE OR REPLACE`) |
| `raw.supplier_prices` | raw | row of a supplier file, every value kept as text, plus `source_file` and `source_row` | table (`CREATE OR REPLACE`) |
| `stg_orders`, `stg_order_items`, `stg_products`, `stg_stores` | staging | order / order line / product / store | view |
| `stg_supplier_prices` | staging | supplier file row: SKU trimmed, cost parsed, nothing filtered | view |
| `int_supplier_price_checks` | silver | supplier file row + `reject_reason` (NULL when usable) | view |
| `int_supplier_costs` | silver | (sku, valid_from): the winning usable quote | view |
| `order_lines` | silver | counted order line (delivered, qty > 0, Riyadh date ≥ 2026-08-31) | table |
| `product_cost_history` | silver | SKU × cost version (SCD2) | table |
| `weekly_margin` | gold | iso_week × city × category with ≥ 1 counted line | table |
| `price_quarantine` | quality | rejected supplier file row | table |

`ingest()` reads the supplier files with Python's `csv` module using `utf-8-sig`, which drops the byte-order mark, and unites the columns by name. That way the week 39 rename (`unit_cost` → `cost_sar`, plus `currency`) needs no special case. It lands every value as text, so `raw_cost` stays exactly as the supplier wrote it. The four app tables are read with `sqlite3`, because DuckDB's sqlite extension would need a download. Every raw table is rebuilt with `CREATE OR REPLACE`, so re-running never duplicates rows. File names come from a glob, so no week or file name is hard-coded.

**How the SCD2 history is built:** `int_supplier_price_checks` gives every file row a reject reason (unknown SKU first, then a cost that isn't positive). Only usable rows go on to `int_supplier_costs`, which keeps one quote per (sku, valid_from). The file whose name sorts last wins, so `prices_2026-W39_fix.csv` replaces `prices_2026-W39.csv` without a special case. `product_cost_history` orders each SKU's quotes by `valid_from` and keeps a quote only if `lag()` shows the cost changed. A week where the SKU is missing therefore produces nothing, and the previous version simply runs on. `valid_to` is `lead(valid_from)`, NULL for the current version.

**How each order line finds its cost:** `weekly_margin` left-joins each counted line to the history on `sku` and `valid_from <= order_date AND (valid_to IS NULL OR order_date < valid_to)`. Versions never overlap, so a line matches at most one version. A line with no match is uncosted: it is counted in `lines`, `revenue_sar` and `uncosted_lines`, but not in `costed_revenue_sar` or `cost_sar`.

`publish()` reads the three dbt tables and compares each with the current Iceberg table, sorted on its key. If they are identical it writes nothing, so a second run creates no new snapshot. If they differ, it swaps the table's data files for the new rows in **one `overwrite` snapshot**, updating the table in place. I didn't use `Table.overwrite()`, which commits an empty `delete` snapshot followed by an `append`, so the snapshot before the current one would hold no rows. With a single snapshot, the current snapshot's parent is always the previous version of the table. A table is created only if it doesn't exist yet.

## Data problems I found

Counts are for the course dataset (seed 2026).

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Byte-order mark at the start of the file | `prices_2026-W38.csv` | 1 file (header) | Read with `utf-8-sig` |
| Windows line endings (`\r\n`) | every supplier file | all 5 files | `csv` module with `newline=""` |
| Decimal commas (`"9,49"`) | W38 | 126 rows | `,` → `.` before casting |
| Arabic-Indic digits (`٩٫٤٩`) | W38 | 34 rows | `translate()` to ASCII digits and `.` |
| Cost column renamed `unit_cost` → `cost_sar`, new `currency` column | W39 and the fix file | 160 + 9 rows | Columns united by name; `coalesce(cost_sar, unit_cost)` |
| Zero or negative costs | W39 | 4 rows | Quarantined `invalid_cost`; the fix file later supplies valid costs |
| SKUs Qafila doesn't sell (`QF-NEW-…`) | W39 | 3 rows | Quarantined `unknown_sku` |
| SKUs with trailing spaces | `products` | 2 products | `trim()` on both sides before matching |
| Same SKU and `valid_from` in two files | W39 vs `W39_fix` | 5 renegotiated prices (+4 fixes for quarantined rows) | Last file name wins |
| Zero or negative quantities | `order_items` | 142 lines | Excluded by `quantity > 0` |
| Order date is in UTC, the business day is Riyadh | `orders.created_at` | 712 orders on a different date than their UTC date (7 delivered orders move from 30 Aug into 31 Aug) | `created_at + 3 h`, then the date |
| Delivered orders before the first supplier week | `orders` | 18,523 orders | Excluded by `order_date >= var('first_supplier_week')` (2026-08-31, set once in `dbt_project.yml`) |
| Lines sold before a SKU's first quote, or never quoted | order lines × supplier files | 20,626 lines, 190 SKUs (28 products never quoted) | Kept as uncosted: in `lines` and revenue, not in cost |

## Tests

`dbt build` runs 33 tests.

- **Tests on silver, gold and quality** protect the rules. They have severity `error`, so a broken rule stops the build before anything is published.
- **Tests on staging** check assumptions about the source data, such as unique SKUs or that every order line has a known product. They have severity `warn`, so an unexpected quirk in a new dataset is reported without stopping the pipeline.

The tests that protect the rules:

| Test | On | What it protects against |
|---|---|---|
| `history_one_row_per_sku_version` | `product_cost_history` | Two versions starting on the same day, e.g. a W39 row and its correction both surviving |
| `history_versions_are_contiguous` | `product_cost_history` | Gaps or overlaps between versions, a wrong `is_current`, or a new version whose cost didn't change |
| `history_costs_are_positive` | `product_cost_history` | Zero or negative costs leaking past the quarantine |
| `quarantine_rows_never_used` | `price_quarantine` × `int_supplier_costs` | A rejected row ending up as a cost version (rule 3) |
| `accepted_values` on `reason` | `price_quarantine` | Any reason other than `unknown_sku` or `invalid_cost` |
| `margin_one_row_per_grain` | `weekly_margin` | Duplicate iso_week × city × category rows |
| `margin_columns_add_up` | `weekly_margin` | `margin ≠ costed revenue − cost`, costed revenue > revenue, uncosted > lines |
| `margin_reconciles_to_order_lines` | `weekly_margin` vs `order_lines` | The point-in-time join losing or double-counting lines (fan-out) |
| `unique` / `not_null` on keys | staging models | Duplicate products, stores or orders multiplying lines |
| `relationships` on `product_id` | `stg_order_items` → `stg_products` | Lines that can't be given a SKU or category |
| `not_null` on `city`, `category`, `order_date` | `order_lines` | Lines falling out of the gold grouping |

## Re-runs and the correction

Run on an empty `warehouse/`, then again, then after `make_data.py --add-correction`:

```
run 1                 silver.product_cost_history: created, 633 rows      gold.weekly_margin: created, 165 rows      quality.price_quarantine: created, 7 rows
run 2 (same data)     silver.product_cost_history: unchanged, 633 rows    gold.weekly_margin: unchanged, 165 rows    quality.price_quarantine: unchanged, 7 rows
run 3 (+ W39_fix)     silver.product_cost_history: overwritten, 633 -> 637 rows   gold.weekly_margin: overwritten, 165 -> 165 rows   quality.price_quarantine: unchanged, 7 rows
run 4 (same data)     all three: unchanged
```

The second run writes no new snapshot. The correction adds one `overwrite` snapshot, whose parent still holds the old numbers. Snapshot list after run 4: `['append', 'overwrite']`.

| `gold.weekly_margin` snapshot | When | Total margin (SAR) | Week 39 cost (SAR) | Week 39 margin (SAR) |
|---|---|---|---|---|
| `640655077669044539` (append) | before the correction | 801,016.88 | 519,954.96 | 248,962.65 |
| `5227919558735769726` (overwrite) | after the correction | 803,515.80 | 518,152.53 | 250,765.08 |

```python
t = get_catalog(Path("warehouse")).load_table("gold.weekly_margin")
t.scan(snapshot_id=t.current_snapshot().parent_snapshot_id).to_pandas()   # pre-correction numbers
```

Snapshot ids change on every fresh run; the numbers don't.

I also checked the outputs against an independent reimplementation of the rules in plain pandas. It is not part of the repo; it doesn't use dbt or DuckDB. On seeds 2026, 11 and 7 (`make_data.py --seed 11 --out data_11`), before and after the correction, it gave:
- every SKU's cost history identical,
- every `weekly_margin` row matching within 0.02 SAR,
- the same quarantine rows.

`tools/check.py` passes 22/22 on all three seeds.

## What I'd do next

- **With 100× more data:** `ingest()` reads every row into Python. I'd switch to DuckDB's `read_csv(..., all_varchar=true, union_by_name=true, filename=true)` and the sqlite scanner, and make `order_lines` and `weekly_margin` incremental by `order_date`. `publish()` compares whole tables in memory. I'd replace that with a hash per partition and overwrite only the weeks that changed (`overwrite_filter`), partitioning `weekly_margin` by `iso_week`.
- **With daily supplier files:** the history logic doesn't change, because it is keyed on `valid_from`, not on weeks. But "the file that sorts last wins" depends on the naming convention. I'd add an explicit arrival timestamp or version to each file and rank on that.

## AI assistance

I did the fork, Claude did the work. (:
