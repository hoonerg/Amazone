# %%
from pymongo import MongoClient
import random
from datetime import datetime, timedelta
from faker import Faker
client = MongoClient('mongodb+srv://roger:roger@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
# %%
# Selecting the collections
products_collection = db['products']
daily_inventory_collection = db['daily_inventory']
# %%
# Creating the storage_warehouses collection
import pgeocode
nomi = pgeocode.Nominatim('gb')
fake = Faker('en_GB')
Faker.seed(33)

# Function to create warehouse data
fake_manchester_postcodes = ['M22 0AJ', 'M15 4AR', 'M13 0JQ', 'M15 4EN', 'M11 0AA', 'M11 0AY']
def generate_warehouse_data(n_warehouses):
    warehouse_data = []
    for i in range(n_warehouses):
        # Generate a random address
        fake.address()
        address = {
            'building_number': fake.building_number(),
            'street': fake.street_name(),
            'city': 'Manchester',
            'postcode': random.choice(fake_manchester_postcodes),
            'country': 'United Kingdom'
        }
        
        # Get geolocation
        location = nomi.query_postal_code(address['postcode'])
        coordinates = {'latitude': location.latitude, 'longitude': location.longitude}
        
        warehouse_data.append({
            'warehouse_name': f'Warehouse {i + 1}',
            'location': coordinates,
            'address': address
        })
    
    return warehouse_data
# %%
current_date = datetime(2023, 12, 15)

# 5 days ago
start_date = current_date - timedelta(days=2)

warehouse_data = generate_warehouse_data(6)

for product in products_collection.find():
    for days in range(2):
        single_date = start_date + timedelta(days=days)

        # Create warehouse inventory list for the product on each date
        warehouses_inventory = []
        for warehouse in warehouse_data:
            warehouses_inventory.append({
                'warehouse_name': warehouse['warehouse_name'],
                'warehouse_location': warehouse['location'],
                'warhouse_address': warehouse['address'],
                'quantity': random.randint(0, 1000)
            })

        # Create the inventory record with the embedded warehouse data
        inventory_record = {
            'product_id': product['_id'],
            'date': single_date,
            'warehouses': warehouses_inventory
        }

        # Insert the record into the daily_inventory collection
        daily_inventory_collection.insert_one(inventory_record)

client.close()