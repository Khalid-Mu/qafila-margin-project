-- Quarantined costs (zero, negative, unparseable) must never reach the history.
select *
from {{ ref('product_cost_history') }}
where unit_cost_sar <= 0
