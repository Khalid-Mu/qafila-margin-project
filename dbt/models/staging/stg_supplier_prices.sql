-- One row per row of a supplier price file, typed and cleaned. Nothing is filtered out here:
-- deciding which rows are usable is the job of silver (quality.price_quarantine / int_supplier_costs).

with source as (

    select * from {{ source('raw', 'supplier_prices') }}

),

renamed as (

    select
        source_file,
        source_row,
        supplier_id,
        trim(sku)                                   as sku,
        try_cast(trim(valid_from) as date)          as valid_from,
        -- the cost column was renamed unit_cost -> cost_sar in week 39; a row only ever has one of them
        coalesce(cost_sar, unit_cost)               as raw_cost
    from source

)

select
    source_file,
    source_row,
    supplier_id,
    sku,
    valid_from,
    raw_cost,
    -- Arabic-Indic digits and decimal separator -> ASCII, then a decimal comma -> point: '٩٫٤٩' and '9,49' -> 9.49
    try_cast(
        replace(translate(trim(raw_cost), '٠١٢٣٤٥٦٧٨٩٫', '0123456789.'), ',', '.')
        as double
    )                                               as unit_cost_sar
from renamed
