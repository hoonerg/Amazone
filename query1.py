from pymongo import MongoClient
from bson.objectid import ObjectId
import geopy.distance
import pgeocode

client = MongoClient('mongodb+srv://hoon:hoon@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
nomi = pgeocode.Nominatim('gb')

## Flow:
# Get order id
# Get items and customer location
# Find nearest store for each item
# Find shortest route for pickup
# Find nearest partner from first point
# Assign partner and modify associate values

def calculate_distance(coords_1, coords_2):
    return geopy.distance.distance(coords_1, coords_2).km

def find_nearest_store_with_item(item_id, customer_location, nearest_stores_dict):
    product = db.products.find_one({"_id": item_id})

    nearest_store_id = None
    min_distance = float('inf')

    for store in db.store.find():
        if any(item['product_id'] == item_id for item in store["grocery_items"]):
            store_location = tuple(store["location"])
            distance = calculate_distance(store_location, customer_location)
            if distance < min_distance:
                min_distance = distance
                nearest_store_id = store['_id']

    if nearest_store_id is not None:
        if nearest_store_id in nearest_stores_dict:
            nearest_stores_dict[nearest_store_id].append(item_id)
        else:
            nearest_stores_dict[nearest_store_id] = [item_id]

    return nearest_stores_dict

def find_nearest_stores(order, customer_location):

    nearest_stores = {}
    for item in order["items"]:
        nearest_stores = find_nearest_store_with_item(item['product_id'], customer_location, nearest_stores)

    return nearest_stores

def calculate_shortest_route(stores, customer_location):
    route = []
    store_locations = {}
    for store_id in stores.keys():
        location = db.store.find_one({'_id': store_id})['location']
        store_locations[store_id] = location

    route.append(customer_location)
    current_point = customer_location
    remaining_stores = list(stores.keys())

    while remaining_stores:
        next_store_id = min(remaining_stores, key=lambda store_id: calculate_distance(current_point, store_locations[store_id]))
        route.append(store_locations[next_store_id])
        current_point = store_locations[next_store_id]
        remaining_stores.remove(next_store_id)

    last_point = current_point
    return route, last_point


def find_nearest_idle_partner(last_point):
    nearest_partner = None
    min_distance = float('inf')

    for partner in db.partners.find({"status": 1, "on_delivery": 0}):
        partner_location = tuple(partner["current_location"])
        distance = calculate_distance(partner_location, last_point)
        if distance < min_distance:
            min_distance = distance
            nearest_partner = partner

    if not nearest_partner:
        return False, None

    return True, nearest_partner

# Main
def process_order(order_id):
    order = db.current_orders.find_one({"_id": order_id, 'segment': 'fresh'})
    customer = db.customer.find_one({"customer_id": order["customer_id"]})
    customer_location_detail = nomi.query_postal_code(customer["shipping_address"]["postcode"])
    customer_location = (customer_location_detail['latitude'], customer_location_detail['longitude'])

    pickup_locations = find_nearest_stores(order, customer_location)
    route, last_point = calculate_shortest_route(pickup_locations, customer_location)
    available, nearest_partner = find_nearest_idle_partner(last_point)

    if available:
        db.partners.update_one({"_id": nearest_partner["_id"]}, {"$set": {"on_delivery": 1, "order_id": order_id}})
        db.current_orders.update_one({"_id": order_id}, {"$set": {"status": "Out for Delivery"}})

        return {
            "available": True,
            "order_id": order_id,
            "delivery_partner_id": nearest_partner["_id"],
            "partner_name": nearest_partner["partner_name"],
            "route": route,
            "final_destination": customer_location
        }
    else:
        return {
            "available": False,
            "order_id": order_id,
            "delivery_partner_id": nearest_partner["_id"],
            "partner_name": nearest_partner["partner_name"],
            "route": route,
            "final_destination": customer_location
        }
        
if __name__ == "__main__":
    query_order_id = ObjectId("656f28947656bde9be7a52c6") #656f28947656bde9be7a52c5 #656f28947656bde9be7a52c6
    output = process_order(query_order_id)
    print(output)