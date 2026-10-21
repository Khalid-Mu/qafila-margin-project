-- Grain: one row per counted order line (rule 7), with what the margin needs:
-- the Riyadh order date, its ISO week, the store's city and the product's category and SKU.

with orders as (

    select * from {{ ref('stg_orders') }}
    where status = 'delivered'
      and order_date >= date '{{ var("first_supplier_week") }}'

),

items as (

    select * from {{ ref('stg_order_items') }}
    where quantity > 0

)

select
    i.order_id,
    i.line_no,
    o.order_date,
    cast(strftime(o.order_date, '%V') as integer)   as iso_week,
    s.city,
    p.category,
    p.sku,
    i.quantity,
    i.line_revenue_sar
from items i
join orders o              on o.order_id = i.order_id
left join {{ ref('stg_stores') }}   s on s.store_id = o.store_id
left join {{ ref('stg_products') }} p on p.product_id = i.product_id
