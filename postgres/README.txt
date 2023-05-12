Created a dataset for the data engineering project.

After setting up postgreSQL server and creating table using
'create_tbl_user_purchase.sql'. Populate the table by running
'populate_database.py script'. 12000 records will be made 
with random combinations of 1-3 items from 1-3 restaurants
in 'restaurant_menu.py' file. The each row of the table
represents an order made by a customer. After 100 records
the date is incremented.

run the script in Windows terminal with command:
py populate_database.py

To do:
Clean up and add config pipeline for python file.  

