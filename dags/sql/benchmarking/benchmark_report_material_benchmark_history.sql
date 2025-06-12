SELECT
    ID,
    USER_ID,
    TIMESTAMP,
    INPUT_DATA,
    PLOTS_DATA,
    TABLE_DATA,
    USER_ID,
    CORRECTED_PRICE,
    CORRECTED_CURRENCY,
    COMPANY_ID,
    
    -- Extracting material_n (manufacturer_part_number)
    input_data->>'manufacturer_part_number' AS material_n,

    -- Extracting price_proposed (vendors.proposed_price)
    (input_data->'vendors'->0->>'proposed_price')::numeric AS price_proposed,

    -- Extracting dmp_result (recommendation_message)
    (plots_data #>> '{recommendation_message}') AS dmp_result,

    -- Extracting price_proposed_old_input (unit_price from table_data array)
    (SELECT unit_price FROM jsonb_array_elements(plots_data->'table_data') AS td 
     ORDER BY (td->>'id')::int DESC LIMIT 1) AS price_proposed_old_input
FROM
    HISTORY_MATERIALBENCHMARKHISTORY