-- No line is lost or double-counted by the point-in-time join: gold adds up to the counted
-- order lines, both in number and in revenue. A fan-out (overlapping versions) would fail here.
with gold as (
    select sum(lines) as lines, round(sum(revenue_sar), 2) as revenue
    from {{ ref('weekly_margin') }}
),
lines as (
    select count(*) as lines, round(sum(line_revenue_sar), 2) as revenue
    from {{ ref('order_lines') }}
)
select *
from gold, lines
where gold.lines <> lines.lines
   or abs(gold.revenue - lines.revenue) > 0.05
