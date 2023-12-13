# Additional Query
# When delivery is completed, move the order to past order and change relevant status
# Input: order_id
# Output: Bool indicating job status

# Flow
# take order id
# get order information from order id
# change 'status' to 'Delivered'
# Move the order to past_order collection
# change on_delivery from 1 to 0
# remove order_id from the partner

from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient('mongodb+srv://hoon:hoon@amazone.4sbahbn.mongodb.net/')
db = client['amazone']

def change_status_and_move_order(db, order_id):
    db['current_orders'].update_one({'_id': order_id}, {'$set': {'status': 'Delivered'}})
    order = db['current_orders'].find_one({'_id': order_id})
    db['past_orders'].insert_one(order)
    db['current_orders'].delete_one({'_id': order_id})

def update_partner_info(db, order_id):
    db['partners'].update_one({'order_id': order_id}, {'$set': {'on_delivery': 0, 'order_id': None}})
    
def process_order(order_id):
    try:
        change_status_and_move_order(db, order_id)
        update_partner_info(db, order_id)
        return True
    except:
        return False

if __name__ == "__main__":
    order_id = ObjectId("657868d6ce3df229b6b2d7bb") #657868d6ce3df229b6b2d7bb
    result = process_order(order_id)
    print("Operation successful:", result)