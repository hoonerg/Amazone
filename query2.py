# Query 2
# At least 1 query that indicates a user searching for available fresh products. The products should be displayed based on the user’s location.
# Input: customer id, product id
# Output: Bool(nearest available store exists or not), nearest available store id

from pymongo import MongoClient
from bson.objectid import ObjectId
import geopy.distance
import pgeocode

client = MongoClient('mongodb+srv://hoon:hoon@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
nomi = pgeocode.Nominatim('gb')

def calculate_distance(coords_1, coords_2):
    return geopy.distance.distance(coords_1, coords_2).km

def find_nearest_store_with_item(product, customer_location):

    nearest_store_id = None
    min_distance = float('inf')
    nearest_stores_dict = {}

    for store in db.store.find():
        if any(item['product_id'] == product['_id'] for item in store["grocery_items"]):
            store_location = tuple(store["location"])
            distance = calculate_distance(store_location, customer_location)
            if distance < min_distance:
                min_distance = distance
                nearest_store_id = store['_id']

    return nearest_store_id

# Main
def process_order(customer_id, product_id):
    customer = db.customer.find_one({"_id": customer_id})
    product = db.products.find_one({"_id": product_id})

    customer_location_detail = nomi.query_postal_code(customer["shipping_address"]["postcode"])
    customer_location = (customer_location_detail['latitude'], customer_location_detail['longitude'])

    nearest_store = find_nearest_store_with_item(product, customer_location)
    if nearest_store == None:
        return {'availability': False,
                'store': None}
    elif nearest_store != None:
        return {'availability': True,
                'store': nearest_store}
        
if __name__ == "__main__":
    print(process_order(ObjectId("656dbed02729ed87da90697e"), ObjectId("65688ffab62ecd780f8b527e"))) # customer_id, product_id