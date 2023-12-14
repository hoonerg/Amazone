from pymongo import MongoClient
import random as rn

client = MongoClient('mongodb+srv://hoon:hoon@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
collection = db['store']

product = {
      "store_name": "Morrisons King Street",
      "store_address": {
        "building_number": "72",
        "street": "Piccadilly",
        "city": "Manchester",
        "postcode": "M4 2AA",
        "country": "UK"
      },
      "location": [53.488296, -2.24107],
      "grocery_items": [
        {"product_id": ObjectId("65688ffab62ecd780f8b527a"), "product_name": "Apple Cider"},
        {"product_id": ObjectId("656ba1e3492bfd49374e0bf8"), "product_name": "Red Velvet Cake"},
        {"product_id": ObjectId("65688ffab62ecd780f8b527c"), "product_name": "Lemonade"},
        {"product_id": ObjectId("656ba1e3492bfd49374e0bf9"), "product_name": "Croissant"},
        {"product_id": ObjectId("656ba1e3492bfd49374e0bfa"), "product_name": "Milk Brownies"},
        {"product_id": ObjectId("656ba1e3492bfd49374e0bf7"), "product_name": "Pain Au Chocolat"},
        {"product_id": ObjectId("65688ffab62ecd780f8b5276"), "product_name": "Orange Juice"}
      ]
    }

collection.insert_one([product])