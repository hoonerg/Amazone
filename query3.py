import pymongo
from random import randint, sample
from bson import ObjectId
from typing import List
import random
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb+srv://sailor:salaar@amazone.4sbahbn.mongodb.net/")
db = client["amazone"]  # Replace "your_database_name" with your actual database name

def add_items_to_cart(product_list: List[dict], customer_id, segment):
    result = db.current_orders.insert_one({
            'items': [],
            'segment': segment,
            'total_amount': 0,
            'customer_id': customer_id,
            'status': 'In Cart'
        })

    order_id = result.inserted_id

    total_amount = 0

    for product_info in product_list:
        product_id = product_info.get("_id")
        quantity = product_info.get("quantity", 0)
        
#         print(product_id, quantity) this code was for debugging

        # Retrieve product details from the products collection
        product_details = db.products.find_one({'_id': ObjectId(product_id)})
#         print(product_details) code for debugging
        
        if product_details:
            product_name = product_details.get('product_name', 'Unknown Product')
            
#             print(product_name) code for debugging
            
            # Update the document with the new item
            db.current_orders.update_one(
                {'_id': order_id},
                {
                    '$push': {
                        'items': {
                            'product_id': product_id,
                            'product_name': product_name,
                            'quantity': quantity
                        }
                    }
                }
            )

            # Update total_amount for the entire cart
            total_amount += product_details["standard_price_to_customers"] * quantity
        else:
            print(f"Product with ID {product_id} not found.")

    # Update total_amount for the cart
    db.current_orders.update_one(
        {'_id': order_id},
        {'$set': {'total_amount': total_amount}}
    )
    return order_id

def make_payment(order_id):
    # Retrieve the total_amount and customer_id from the current_orders document
    order = db.current_orders.find_one({'_id': ObjectId(order_id)})
    
    if order:
        total_amount = order.get('total_amount', 0.0)
        customer_id = order.get('customer_id')
        
#         print(f"Retrieved customer_id from current_orders: {customer_id}")  # Debugging print
        
        if customer_id:
            # Convert customer_id to integer
            customer_id = int(customer_id)
            
#             print(f"Attempting to find customer in customers collection with ID: {customer_id}")  # Debugging print
            
            # Get the current credit_score from the customers collection
            customer = db.customers.find_one({'customer_id': customer_id})
#             print(f"Customer document from customers collection: {customer}")  # Debugging print

            if customer:
                current_credit_score = customer.get('customer_credit', 0)
                
                # Update the credit_score based on the payment amount
                new_credit_score = current_credit_score - total_amount

                # Update the customer document with the new credit_score
                db.customers.update_one(
                    {'customer_id': customer_id},
                    {
                        '$set': {
                            'customer_credit': new_credit_score
                        }
                    }
                )
                
                # Update the order status to 'Paid' in current_orders
                db.current_orders.update_one(
                    {'_id': ObjectId(order_id)},
                    {
                        '$set': {
                            'status': 'Paid',
                            'payment_timestamp': datetime.now(),
                        }
                    }
                )

            else:
                print(f"Customer with ID {customer_id} not found in the customers collection.")
        else:
            print("Customer ID is missing in the order.")
    else:
        print(f"Order with ID {order_id} not found.")


# Query 1: Creating a Cart with Fresh Products
segment = 'fresh'

product_list = [
    {"_id": str(ObjectId("65688ffab62ecd780f8b527e")), "quantity": 10},
    {"_id": str(ObjectId("65688ffab62ecd780f8b5275")), "quantity": 2},
    {"_id": str(ObjectId("656ba1e3492bfd49374e0bf5")), "quantity": 2},
    {"_id": str(ObjectId("656ba1e3492bfd49374e0bfb")), "quantity": 5}
    # Add more products as needed
]
customer_id = 15 # this is the customer_id

# Customer Cart is created and order_id is stored in query1_order
query1_order = add_items_to_cart(product_list, customer_id, segment)

# Payment is made on this order
make_payment(query1_order)

# Query 2: Creating a Cart with Other Products
segment = 'others'

product_list = [
    {"_id": str(ObjectId("656db6fb9f73564819627c71")), "quantity": 1},
    {"_id": str(ObjectId("656db5859f73564819627c6f")), "quantity": 1},
    {"_id": str(ObjectId("65688c40b62ecd780f8b526a")), "quantity": 1}
    # Add more products as needed
]
customer_id = 3 # this is the customer_id

query2_order = add_items_to_cart(product_list, customer_id, segment)

make_payment(query2_order)