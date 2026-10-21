-- Grain: one row per rejected supplier file row.
-- Rejected rows stay here even after a correction file supersedes them: they are still rows the
-- supplier sent that we could not use.

select
    source_file,
    source_row,
    sku,
    valid_from,
    raw_cost,
    reject_reason as reason
from {{ ref('int_supplier_price_checks') }}
where reject_reason is not null
