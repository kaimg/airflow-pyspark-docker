WITH filtered_materials AS (
    SELECT 
        manufacturer_name,
        manufacturer_part_no,
        customer_long_description AS description,
        po_item_quantity,
        po_item_first_release_date
    FROM 
        materials_table
    WHERE 
        region = 'AGT'
        AND company_id = 1
),

current_year_data AS (
    SELECT 
        EXTRACT(YEAR FROM CURRENT_DATE) AS current_year
),

yearly_demand AS (
    SELECT
        m.manufacturer_name,
        m.manufacturer_part_no,
        m.description,
        EXTRACT(YEAR FROM m.po_item_first_release_date) AS order_year,
        SUM(m.po_item_quantity) AS total_quantity
    FROM 
        materials_table m
    INNER JOIN filtered_materials f
        ON m.manufacturer_name = f.manufacturer_name
        AND m.manufacturer_part_no = f.manufacturer_part_no
        AND m.description = f.description
    WHERE 
        m.region = 'AGT'
        AND m.company_id = 1
    GROUP BY 
        m.manufacturer_name,
        m.manufacturer_part_no,
        m.description,
        EXTRACT(YEAR FROM m.po_item_first_release_date)
)

SELECT
    p.id AS contract_product_id,
    MAX(CASE WHEN y.order_year = c.current_year - 1 THEN y.total_quantity END) AS last_year_demand,
    AVG(y.total_quantity) FILTER (WHERE y.order_year BETWEEN c.current_year - 3 AND c.current_year - 1) AS average_demand
FROM 
    contract_dashboard_pricebook_and_pricebook_product_info p
LEFT JOIN yearly_demand y
    ON p.manufacturer_name = y.manufacturer_name
    AND p.manufacturer_part_no = y.manufacturer_part_no
    AND p.customer_long_description = y.description
CROSS JOIN current_year_data c
GROUP BY 
    p.id, c.current_year;