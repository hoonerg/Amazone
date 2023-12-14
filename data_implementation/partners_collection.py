from pymongo import MongoClient

client = MongoClient('mongodb+srv://hoon:hoon@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
collection = db['partners']

partner = {
        "partner_name": "Lily Smith",
        "partner_age": 40,
        "status": 0,
        "current_location": [53.49176, -2.23793],
        "on_delivery": 0,
        "order_id": None,
        "payout": 15
    }

collection.insert_one(partner)