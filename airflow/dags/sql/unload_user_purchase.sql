-- dags/sql/unload_user_purchase.sql
-- from user_purchase table grab data between dates
-- turn it into csv file 
copy (
       select 
       invoice_number,
       vendor_id,
       delivery_method,
       menu_items,
       invoice_date,
       tax,
       total,
       customer_id
       from user_purchase 
       where invoice_date
       between
       '{{params.begin_date}}'
       and 
       '{{params.end_date}}'
) TO '{{ params.user_purchase }}' WITH (FORMAT CSV, HEADER);