DROP TABLE IF EXISTS user_purchase;

CREATE TABLE user_purchase(
invoice_number SERIAL PRIMARY KEY, 
vendor_id int NOT NULL,
delivery_method TEXT CHECK (delivery_method IN ('pickup','delivery')),
menu_items VARCHAR(200),
invoice_date date,
tax NUMERIC(5,5),
total NUMERIC CHECK(total >= 0),
country_code_iso3 char(3) DEFAULT NULL,
customer_id int NOT NULL
);

