INSERT INTO public.pricebook_analytics_staging (
    product_id,
    product_name,
    pricebook_id,
    pricebook_name,
    unit_price,
    currency,
    effective_date,
    end_date,
    last_updated_at
)
SELECT
    pricebooks_pricebookproduct.id,
    pricebooks_pricebookproduct.pricebook_id,
    pricebooks_pricebookproduct.status,
    pricebooks_pricebookproduct.is_new,
    pricebooks_pricebookproduct.cam_approved,
    pricebooks_pricebookproduct.disable_reason,
    pricebooks_pricebookproduct.updated_at,
    pricebooks_pricebookproduct.product_id,
    pricebooks_pricebookproduct.is_new_for_contract_owner,

    MAX(CASE WHEN pb_required_field.field_name = 'supplier_uom' THEN pricebooks_pricebookvalues.field_value END) AS supplier_uom,
    MAX(CASE WHEN pb_required_field.field_name = 'supplier_description' THEN pricebooks_pricebookvalues.field_value END) AS supplier_description,
    MAX(CASE WHEN pb_required_field.field_name = 'supplier_part_no' THEN pricebooks_pricebookvalues.field_value END) AS supplier_part_no,
    MAX(CASE WHEN pb_required_field.field_name = 'unit_price' THEN pricebooks_pricebookvalues.field_value END) AS unit_price,
    MAX(CASE WHEN pb_required_field.field_name = 'price_valid_from' THEN pricebooks_pricebookvalues.field_value END) AS price_valid_from,
    MAX(CASE WHEN pb_required_field.field_name = 'price_valid_to' THEN pricebooks_pricebookvalues.field_value END) AS price_valid_to,
    MAX(CASE WHEN pb_required_field.field_name = 'sla_lead_time' THEN pricebooks_pricebookvalues.field_value END) AS sla_lead_time,
    MAX(CASE WHEN pb_required_field.field_name = 'incoterm_key' THEN pricebooks_pricebookvalues.field_value END) AS incoterm_key,
    MAX(CASE WHEN pb_required_field.field_name = 'minimum_order_quantity' THEN pricebooks_pricebookvalues.field_value END) AS minimum_order_quantity,
    MAX(CASE WHEN pb_required_field.field_name = 'maximum_order_quantity' THEN pricebooks_pricebookvalues.field_value END) AS maximum_order_quantity,
    MAX(CASE WHEN pb_required_field.field_name = 'customer_short_description' THEN pricebooks_pricebookvalues.field_value END) AS customer_short_description,
    MAX(CASE WHEN pb_required_field.field_name = 'sap_id' THEN pricebooks_pricebookvalues.field_value END) AS customer_unique_id,
    MAX(CASE WHEN pb_required_field.field_name = 'manufacturer_name' THEN pricebooks_pricebookvalues.field_value END) AS manufacturer_name,
    MAX(CASE WHEN pb_required_field.field_name = 'manufacturer_part_no' THEN pricebooks_pricebookvalues.field_value END) AS manufacturer_part_no,
    MAX(CASE WHEN pb_required_field.field_name = 'manufacturer_description' THEN pricebooks_pricebookvalues.field_value END) AS manufacturer_description,

    pricebooks_pricebook.contract_id AS contract_id,
    pricebooks_pricebook.description AS pricebook_description,
    pricebooks_pricebook.currency AS pricebook_currency,
    pricebooks_pricebook.customer_reference_number AS pricebook_customer_reference_number,
    NULL AS last_download_date -- Placeholder, or calculate if needed
FROM
    pricebooks_pricebookproduct
JOIN
    pricebooks_pricebookvalues
    ON pricebooks_pricebookproduct.product_id = pricebooks_pricebookvalues.pb_product_id
JOIN
    pb_required_field
    ON pb_required_field.id = pricebooks_pricebookvalues.pb_required_field_id
INNER JOIN pricebooks_pricebook
    ON pricebooks_pricebookproduct.pricebook_id = pricebooks_pricebook.id
GROUP BY
    pricebooks_pricebookproduct.id,
    pricebooks_pricebookproduct.pricebook_id,
    pricebooks_pricebookproduct.status,
    pricebooks_pricebookproduct.is_new,
    pricebooks_pricebookproduct.cam_approved,
    pricebooks_pricebookproduct.disable_reason,
    pricebooks_pricebookproduct.updated_at,
    pricebooks_pricebookproduct.product_id,
    pricebooks_pricebookproduct.is_new_for_contract_owner,
    pricebooks_pricebook.contract_id,
    pricebooks_pricebook.description,
    pricebooks_pricebook.currency,
    pricebooks_pricebook.customer_reference_number;