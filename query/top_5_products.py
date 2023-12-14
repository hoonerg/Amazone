# Requires the PyMongo package.
# https://api.mongodb.com/python/current

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
        '$group': {
            '_id': '$items.product_id', 
            'product_name': {
                '$first': '$product.product_name'
            }, 
            'total_quantity': {
                '$sum': '$items.quantity'
            }, 
            'total_revenue': {
                '$sum': {
                    '$multiply': [
                        '$product.standard_price_to_customers', '$items.quantity'
                    ]
                }
            }
        }
    }, {
        '$sort': {
            'total_revenue': -1
        }
    }, {
        '$limit': 5
    }, {
        '$project': {
            '_id': 0, 
            'product_id': '$_id', 
            'product_name': 1, 
            'total_quantity': 1, 
            'total_revenue': 1
        }
    }
])

for document in result:
    print(document)
