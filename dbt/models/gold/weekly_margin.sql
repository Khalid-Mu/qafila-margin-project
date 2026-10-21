-- Grain: one row per iso_week x city x category with at least one counted order line.
--
-- Point-in-time costing (rule 9): each line takes the cost version that was valid on its order date,
-- valid_from <= order_date < valid_to (open-ended for the current version). A line with no such version
-- is uncosted: it counts in lines and revenue_sar but not in the cost columns.

with costed_lines as (

    select
        l.*,
        h.unit_cost_sar,
        l.quantity * h.unit_cost_sar as line_cost_sar
    from {{ ref('order_lines') }} l
    left join {{ ref('product_cost_history') }} h
        on  h.sku = l.sku
        and h.valid_from <= l.order_date
        and (h.valid_to is null or l.order_date < h.valid_to)

),

weekly as (

    select
        iso_week,
        city,
        category,
        count(*)                                                                as lines,
        sum(line_revenue_sar)                                                   as revenue_sar,
        coalesce(sum(case when unit_cost_sar is not null then line_revenue_sar end), 0)
                                                                                as costed_revenue_sar,
        coalesce(sum(line_cost_sar), 0)                                         as cost_sar,
        count(*) filter (where unit_cost_sar is null)                           as uncosted_lines
    from costed_lines
    group by iso_week, city, category

)

select
    cast(iso_week as integer)                                   as iso_week,
    city,
    category,
    cast(lines as integer)                                      as lines,
    round(revenue_sar, 2)                                       as revenue_sar,
    round(costed_revenue_sar, 2)                                as costed_revenue_sar,
    round(cost_sar, 2)                                          as cost_sar,
    round(round(costed_revenue_sar, 2) - round(cost_sar, 2), 2) as margin_sar,
    cast(uncosted_lines as integer)                             as uncosted_lines
from weekly
