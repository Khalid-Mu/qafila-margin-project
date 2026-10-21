-- Each version ends exactly where the next one starts, only the last one is current,
-- and a new version always changes the cost. Any row returned breaks SCD2.
with h as (
    select
        *,
        lead(valid_from)    over (partition by sku order by valid_from) as next_from,
        lag(unit_cost_sar)  over (partition by sku order by valid_from) as prev_cost
    from {{ ref('product_cost_history') }}
)
select *
from h
where valid_to is distinct from next_from
   or is_current <> (valid_to is null)
   or valid_to <= valid_from
   or unit_cost_sar = prev_cost
