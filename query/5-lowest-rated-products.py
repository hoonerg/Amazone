#this query returns the id, name and rating of the five lowest ranked products

#importing
import pymongo
from pymongo import MongoClient 

#connecting to MongoDB
conn_str = "mongodb+srv://julius:julius@amazone.4sbahbn.mongodb.net/"
client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
db = client['amazone']
rating_collection = db.product_rating


#Creating Pipeline
pipeline = [
    {
        "$group": {
            "_id": "$product_id",  
            "product_name": {"$first": "$product_name"},
            "average_rating": {"$avg": "$rating"} 
        }
    },
    {
        "$sort": {"average_rating": 1}
    },
    {
        "$limit": 5 
    }
]

#Running Query
lowest_rated_products = rating_collection.aggregate(pipeline)

#Printing results
for product in lowest_rated_products:
    print(f"Product ID: {product['_id']}, Product Name: {product['product_name']}, Average Rating: {product['average_rating']}")
