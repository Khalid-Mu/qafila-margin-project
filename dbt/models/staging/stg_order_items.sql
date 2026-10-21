-- One row per order line.

select
    order_id,
    line_no,
    product_id,
    quantity,
    unit_price,
    discount,
    round(quantity * unit_price - discount, 2)  as line_revenue_sar
from {{ source('raw', 'order_items') }}
