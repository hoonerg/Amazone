from faker import Faker
from pymongo import MongoClient
import random as rn
import string
from datetime import datetime, timedelta
import pytz
# SEED MUST BE 42!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#42
rn.seed(42)

# Connect to MongoDB
client = MongoClient('mongodb+srv://edwinciw:e13d97c46@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
products_doc = db['products']
past_doc = db['past_orders']
cur_doc = db['current_orders']


# Create an instance of the Faker class
fake = Faker()

"""
PAST ORDERS
"""
def random_datetime(start_date, end_date):
    """
    Generate a random datetime object within the specified range.
    
    Parameters:
    - start_date (datetime): The start of the time range.
    - end_date (datetime): The end of the time range.
    
    Returns:
    - A randomly generated datetime object within the specified range. 
    
    """
    delta = end_date - start_date
    time_dict = {
        'days' : rn.randint(0, delta.days),
        'seconds': rn.randint(0, delta.seconds),
        'microseconds': rn.randint(0, 999999)
        }
    
    return start_date + timedelta(**time_dict)

# Example usage:
start_date = datetime(2023, 11, 1, 0, 0, 0)  # Replace with your desired start date
end_date = datetime(2023, 12, 13, 23, 59, 59)  # Replace with your desired end date

random_date = random_datetime(start_date, end_date)
print("Randomly generated datetime:", random_date)

# Delete every document in a collection
past_doc.delete_many({})

# Past orders schema
cus_candidates = list(range(1,21))
past_orders = []

rn.seed(12)
fresh_id_name = products_doc.find({'segment' : 'fresh'}, {'_id' : 1, 'product_name' : 1})
others_id_name = products_doc.find({'segment' : 'others'}, {'_id' : 1, 'product_name' : 1})


# Storing the fresh and other products into separate lists
fresh = []
others = []
for x in fresh_id_name:
    fresh.append(x)

for x in others_id_name:
    others.append(x)
    
boolean = ('fresh', 'others')





# Begin generating instances for past_orders
for i in cus_candidates:
    for j in range(5):
        items_list = []
        k = rn.randint(1,6)
        
        segment = rn.sample(boolean, 1)
        if  segment[0] == 'fresh':
            random_prod = rn.sample(globals()['fresh'], k)
        else:
            random_prod = rn.sample(globals()['others'], k)
        for n in range(k):
            num = rn.randint(1,5)
            item = {
                "product_id":  random_prod[n]['_id'],
                "product_name": random_prod[n]['product_name'],
                "quantity": num
            }
            items_list.append(item)
            
        past_order = {
            "items": items_list,
            "segment": segment[0],
            "total_amount" : 0,
            "customer_id": i,
            "status": 'Delivered',
            "payment_timestamp" : random_datetime(datetime(2023, 11, 1, 0, 0, 0), datetime(2023, 12, 13, 23,59,59))
        }
        past_orders.append(past_order)
        
for x in past_orders:
    past_doc.insert_one(x)




"""
CURRENT ORDER
"""
# Current orders schema
status = ['In Cart', 'Out For Delivery', 'Paid']
cus_candidates = list(range(1,21))
cur_orders = []

# Delete every document in a collection
cur_doc.delete_many({})

# Begin generating instances for current_orders
for i in cus_candidates:
    for j in range(2):
        items_list = []
        k = rn.randint(1,6)
        
        segment = rn.sample(boolean, 1)
        if  segment[0] == 'fresh':
            random_prod = rn.sample(globals()['fresh'], k)
        else:
            random_prod = rn.sample(globals()['others'], k)
            
        for n in range(k):
            num = rn.randint(1,5)
            item = {
                "product_id":  random_prod[n]['_id'],
                "product_name":random_prod[n]['product_name'],
                "quantity": num
            }
            items_list.append(item)
        
            
        cur_order = {
            "items": items_list,
            "segment" : segment[0],
            "total_amount" : 0,
            "customer_id": i,
            "status": rn.sample(status, 1)[0],
            "payment_timestamp" : random_datetime(datetime(2023, 12, 14, 0, 0, 0), datetime(2023, 12, 14, 23,59,59))
        }
        cur_orders.append(cur_order)
        
for x in cur_orders:
    cur_doc.insert_one(x)


# Current order and past order total amount aggregation pipeline
# Use it right after generating the documents on the collections current_orders and past_orders

"""
# Main aggregation
# use db.current_orders and db.past_orders separately on Mongosh
db.past_orders.aggregate([
    { $unwind: "$items" },
    {
        $lookup: {
            from: "products",
            localField: "items.product_id",
            foreignField: "_id",
            as: "product_info"
        }
    },
    { $unwind: "$product_info" },
    {
        $set: {
            "items.total_cost": {
                $multiply: ["$items.quantity", "$product_info.standard_price_to_customers"]
            }
        }
    },
    {
        $group: {
            _id: "$_id",
            items: {
                $push: "$items",
            },
            total_amount: {
                $sum: "$items.total_cost"
            },
            customer_id: {
                $first: "$customer_id"
            },
            status: {
                $first: "$status"
            }
        }
    },
    {
      $unset : 'items.total_cost'
     },
    {
        $merge: {
            into: "past_orders",  //when db.current_orders, replace "past_orders" with "current_orders"
            whenMatched: "merge"
        }
    }
]);
"""
