-- Internal consistency of every margin row.
select *
from {{ ref('weekly_margin') }}
where abs(costed_revenue_sar - cost_sar - margin_sar) > 0.01
   or costed_revenue_sar > revenue_sar + 0.01
   or uncosted_lines > lines
   or lines <= 0
