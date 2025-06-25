WITH unit_price_fields AS (
    SELECT id
    FROM pb_required_field
    WHERE field_name = 'unit_price'
),
request_value_with_unit_price AS (
    SELECT rv.*
    FROM request_values rv
    JOIN unit_price_fields upf ON rv.pricebook_value_id = upf.id
),
request_product_history_ordered AS (
    SELECT *, LEAD(status) OVER (PARTITION BY id ORDER BY history_date) AS next_status
    FROM requests_historicalrequest
),
resend_events AS (
    SELECT rph.id AS request_product_id,
           rph.history_date AS resend_date
    FROM request_product_history_ordered rph
),
request_product_data AS (
    SELECT rp.id AS request_product_id,
           rp.request_id,
           rp.pricebook_product_id,
           rp.type,
           rp.status,
           rp.history_date AS request_created_at
    FROM requests_historicalrequestproduct rp
    WHERE rp.type = 'COMMERCIAL'
),
request_value_history_with_user AS (
    SELECT rvh.*,
           EXTRACT(EPOCH FROM rvh.history_date) AS history_epoch
    FROM requests_historicalrequestvalue rvh
)
SELECT
    rp.pricebook_product_id AS product_id,
    rp.request_id,
    rp.status,
    rvh.draft_value AS unit_price,
    rvh.history_user_id,
    rvh.history_date,
    CASE
        WHEN rvh.history_date <= rp.request_created_at THEN 'previous_approved'
        WHEN rp.status = 'APPROVED' AND rvh.history_date >= ALL (
            SELECT history_date
            FROM requests_historicalrequestvalue
            WHERE request_product_id = rp.request_product_id
        ) THEN 'approved'
        ELSE CONCAT('PENDING_', ROW_NUMBER() OVER (
            PARTITION BY rp.request_product_id
            ORDER BY rvh.history_date
        ))
    END AS status_label
FROM request_product_data rp
JOIN request_value_with_unit_price rv ON rp.request_product_id = rv.request_product_id
JOIN request_value_history_with_user rvh ON rv.id = rvh.id
LEFT JOIN resend_events re ON rp.request_product_id = re.request_product_id AND rvh.history_date < re.resend_date
WHERE rvh.history_date <= NOW()
ORDER BY rp.request_product_id, rvh.history_date;