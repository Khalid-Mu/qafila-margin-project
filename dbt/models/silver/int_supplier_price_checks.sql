{{ config(materialized='view') }}

-- One row per supplier file row, with the reason it can't be used (NULL when it can).
-- Rule 3: unknown SKU is checked first; only a known SKU can have an invalid cost.
-- The single source of truth for both quality.price_quarantine and the usable costs.

with prices as (

    select * from {{ ref('stg_supplier_prices') }}

),

known_skus as (

    select distinct sku from {{ ref('stg_products') }}

)

select
    p.*,
    case
        when k.sku is null                                       then 'unknown_sku'
        when p.unit_cost_sar is null or p.unit_cost_sar <= 0     then 'invalid_cost'
    end as reject_reason
from prices p
left join known_skus k on k.sku = p.sku
