{{ config(materialized='view') }}

-- Grain: one row per (sku, valid_from): the cost quote that wins.
-- Rule 5: when several usable rows share a SKU and valid_from, the file whose name sorts last wins,
-- so prices_2026-W39_fix.csv replaces prices_2026-W39.csv. Quarantined rows never compete.

with usable as (

    select * from {{ ref('int_supplier_price_checks') }}
    where reject_reason is null

),

ranked as (

    select
        *,
        row_number() over (
            partition by sku, valid_from
            order by source_file desc, source_row desc
        ) as rank_in_key
    from usable

)

select
    sku,
    valid_from,
    round(unit_cost_sar, 2) as unit_cost_sar,
    source_file
from ranked
where rank_in_key = 1
