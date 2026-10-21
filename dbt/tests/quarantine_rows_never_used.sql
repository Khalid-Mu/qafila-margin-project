-- A quarantined row must not be the quote behind any cost version (rule 3).
select q.*
from {{ ref('price_quarantine') }} q
join {{ ref('int_supplier_costs') }} c
  on c.source_file = q.source_file
 and c.sku = q.sku
 and c.valid_from = q.valid_from
