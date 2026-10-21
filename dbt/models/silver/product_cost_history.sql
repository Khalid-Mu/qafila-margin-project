-- SCD2 history of supplier costs. Grain: one row per SKU per cost version.
--
-- A weekly file only lists the SKUs quoted that week. A SKU that is missing from a week simply has no
-- quote that week, so its previous version keeps running (rule 4). A quote that repeats the current cost
-- is not a new version (rule 6). valid_to is the next version's valid_from (exclusive), NULL when current.

with quotes as (

    select
        sku,
        valid_from,
        unit_cost_sar,
        lag(unit_cost_sar) over (partition by sku order by valid_from) as previous_cost
    from {{ ref('int_supplier_costs') }}

),

versions as (

    -- keep the first quote of each SKU and every quote that changes the cost
    select sku, valid_from, unit_cost_sar
    from quotes
    where previous_cost is null or unit_cost_sar <> previous_cost

)

select
    sku,
    cast(unit_cost_sar as double)                                               as unit_cost_sar,
    valid_from,
    lead(valid_from) over (partition by sku order by valid_from)               as valid_to,
    lead(valid_from) over (partition by sku order by valid_from) is null       as is_current
from versions
