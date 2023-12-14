# Requires the PyMongo package.
# https://api.mongodb.com/python/current

from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient('mongodb+srv://sailor:salaar@amazone.4sbahbn.mongodb.net/')
result = client['amazone']['past_orders'].aggregate([
    {
        '$unwind': '$items'
    }, {
        '$lookup': {
            'from': 'products', 
            'localField': 'items.product_id', 
            'foreignField': '_id', 
            'as': 'product'
        }
    }, {
        '$unwind': '$product'
    }, {
        '$project': {
            'product_name': '$product.product_name', 
            'quantity': '$items.quantity', 
            'price': '$product.standard_price_to_customers', 
            'total_amount': {
                '$multiply': [
                    '$items.quantity', '$product.standard_price_to_customers'
                ]
            }
        }
    }, {
        '$sort': {
            'total_amount': -1
        }
    }, {
        '$limit': 5
    }
])
