-- One row per product. SKUs are trimmed so they match the supplier files.

select
    product_id,
    trim(sku)       as sku,
    category,
    name_en
from {{ source('raw', 'products') }}
