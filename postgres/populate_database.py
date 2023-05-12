

#Python imports
import datetime
# widely used PostgreSQL connector library
import psycopg2
#pip install psycopg2
import random
from decimal import Decimal
from restaurant_menu import McDonald_menu, Wingstop_menu, Taco_bell_menu

delivery_list = ['pickup', 'delivery']

vendor_dict = { 1: McDonald_menu,
             2: Wingstop_menu,
             3: Taco_bell_menu,
    
}

country_code_iso3 = 'USA'
customer_id_list = list(range(1,10001))

RECORDS = 12001
REC_PER_DATE = 100


tax_list = [0,.029,.04,.04225,.0445,.045,.0475,.05,.053,.055,.056,.0575,.06,.061,.0625,.0635,.065,.06625,.0685,.06875,.07,.0725]

#establish the connection
conn = psycopg2.connect(
    host="localhost",
    port='5432',
    database="DE",
    user="postgres", 
    password="password")

#set auto commit 
conn.autocommit = True

#create a cursor object
cur = conn.cursor()

def insert(column_names, column_values, cur):
    query = f"""INSERT INTO user_purchase
    ({column_names})
    VALUES
    ({column_values});
    """
    #execute the INSERT statement
    cur.execute(query)

cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")

#check if table is in database
for table in cur.fetchall():
    print(table)

#delete old table
query = """DELETE FROM user_purchase;
"""

cur.execute(query)

column_names="vendor_id, delivery_method, menu_items, invoice_date, tax, total, country_code_iso3, customer_id"


def getMenu(vendor):
    return vendor_dict[vendor]
        


#Get the total from the dictionary values
#Since we need precision use Decimal
def getDictTotal(item_dict):
    values_list = item_dict.values()
    total = Decimal(0)
    for value in values_list:
        num = Decimal(value)
        total += num
    return total

def checkKey(item_dict, key):
    #check if empty
    #empty dictionary are false 
    if (bool(item_dict) == False):
        return False
    elif key in item_dict.keys():
        return True
    else:
        return False

def addItem(item_dict, menu):

    rnum = random.randrange(0,len(menu))
    key = list(menu)[rnum]
    val = list(menu.values())[rnum]
    #check if key already in item_dict
    isDuplicate = checkKey(item_dict, key)
    
   # try:
        #if duplicate  
    if(isDuplicate):
        x = item_dict[key]
        
        #check if x is zero
        #if it's zero do nothing
        #add a way to count zero items later
        if(x!=0):
        
            #check number of duplicates
            count = val/x
            #round to nearest integer 
            count = round(count)
            
            #example new burger is now
            # (1) Double Burger
            key = f"({count}) {key}"
            item_dict[key] = val
    #not duplicate add new key-value pair
    else:

        item_dict[key]=val

    #except (Exception) as error:
    #    print(error)
        
    
    return item_dict


def getItemList(menu, rnum, item_dict ):

    item_dict = item_dict
    if(rnum > 0):
        item_dict = addItem(item_dict, menu)
        getItemList(menu, rnum-1, item_dict)
    return item_dict

def print_date(date_obj):
    date_format="%Y-%m-%d"
    print(date_obj.strftime(date_format))

def increment_date(date_obj):
    date_obj += datetime.timedelta(days=1)
    return date_obj


def create_date_obj(year, month, day):
    date_obj = datetime.datetime(year, month, day)
    return date_obj

def get_col_2(start, end):
    rnum = random.randrange(start, end)
    return rnum

def get_col_3(start, end):
    rnum = random.randrange(0,2)
    delivery_method = delivery_list[rnum]
    return delivery_method

#get number of items ordered from 1 - end
def get_items_ordered(start, end):
    rnum = random.randrange(start, end)
    return rnum

def get_csv_from_dict(dictionary):
    new_list = list(dictionary)
    new_csv = '|'.join(new_list)
    return new_csv

def get_customer_id(rnum):
    customer_id = customer_id_list[rnum]
    return customer_id


def get_col_9(start, end):
    rnum= random.randrange(start,end)
    customer_id = get_customer_id(rnum)
    return customer_id

def get_item_dict(vendor_id, start,end):
    
    items_ordered = get_items_ordered(start, end)
    menu_dict = getMenu(vendor_id) #get menu dictionary

    item_dict={}
    item_dict = getItemList(menu_dict,items_ordered, item_dict)
    
    return item_dict

def get_col_6():
    rnum = random.randrange(0, len(tax_list))
    tax = tax_list[rnum]
    return tax

def get_col_7(item_dict, tax):
    item_total = getDictTotal(item_dict)
    item_total = round(item_total, 2)

    #convert to decimal
    tax = Decimal(tax)
    item_tax =  item_total* tax
    #round number to closest two decimal places
    item_tax = round(item_tax,2)

    total = item_total + item_tax
    return total

date_obj = create_date_obj(2023,1,1)

for i in range(1,RECORDS):
    
    if((i%REC_PER_DATE) == 0):
        date_obj = increment_date(date_obj)

    vendor_id = get_col_2(1,4)
    col_2=vendor_id
    
    delivery_method = get_col_3(0,2)
    col_3=delivery_method
        
    
    item_dict = get_item_dict(vendor_id, 1,4)
    item_csv = get_csv_from_dict(item_dict)
    col_4=item_csv
    
    col_5=date_obj

    tax = get_col_6()
    col_6= tax
    
    
    total =get_col_7(item_dict, tax)
    col_7=total
    
    col_8=country_code_iso3

    customer_id = get_col_9(0, 10000)
    col_9= customer_id
    
    column_values = f" {col_2} , '{col_3}' , '{col_4}' ,'{col_5}' ,{col_6} ,{col_7} ,'{col_8}', {col_9}"

    
    insert(column_names, column_values, cur)

column_values = f" {col_2} , '{col_3}' , '{col_4}' ,'{col_5}' ,{col_6} ,{col_7} ,'{col_8}', {col_9}"

def get_last_row(cur):
    #get last row of user_purchase
    query = """SELECT * FROM user_purchase
    WHERE invoice_number=(SELECT max(invoice_number) FROM user_purchase);
    """
    cur.execute(query)
    result = cur.fetchone()
    return result

result = get_last_row(cur)
print(result[0])

def get_all_records(cur):
    query = """SELECT * FROM user_purchase;"""
    cur.execute(query)
    result = cur.fetchall()
    return result

def get_tbl_count(cur):
    query = """SELECT COUNT(*) FROM user_purchase """
    cur.execute(query)
    result = cur.fetchone()
    print(result)

get_tbl_count(cur)

#Closing the connection
conn.close()

