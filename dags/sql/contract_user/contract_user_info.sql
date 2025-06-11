-- SELECT
--     u.id,
--     u.first_name,
--     u.last_name,
--     c.id AS company_id,
--     c.name AS company_name,
--     r.name AS user_type
-- FROM u
-- LEFT JOIN c ON u.company_id = c.id
-- LEFT JOIN users_user_roles ur ON u.id = ur.user_id
-- LEFT JOIN roles_role r ON ur.role_id = r.id

SELECT * FROM currency_rates