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
    -- material_n
    INPUT_DATA->>'manufacturer_part_number' AS material_n,

    -- price_proposed
    (INPUT_DATA->'vendors'->0->>'proposed_price')::numeric AS price_proposed,

    -- dmp_result
    PLOTS_DATA #>> '{recommendation_message}' AS dmp_result,

    -- price_proposed_old_input (latest unit_price from table_data JSON array)
    (SELECT (td->>'unit_price')::numeric
     FROM jsonb_array_elements(TABLE_DATA) AS td
     ORDER BY (td->>'id')::int DESC
     LIMIT 1) AS price_proposed_old_input
FROM
    HISTORY_MATERIALBENCHMARKHISTORY