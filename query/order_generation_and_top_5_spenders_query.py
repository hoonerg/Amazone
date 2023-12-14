from faker import Faker
from pymongo import MongoClient
import random as rn
import string
from datetime import datetime, timedelta
import pytz
# SEED MUST BE 42!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#42
rn.seed(42)

# Connect to MongoDB
client = MongoClient('mongodb+srv://edwinciw:e13d97c46@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
products_doc = db['products']
past_doc = db['past_orders']
cur_doc = db['current_orders']


# Create an instance of the Faker class
fake = Faker()

"""
PAST ORDERS
"""
def random_datetime(start_date, end_date):
    """
    Generate a random datetime object within the specified range.
    
    Parameters:
    - start_date (datetime): The start of the time range.
    - end_date (datetime): The end of the time range.
    
    Returns:
    - A randomly generated datetime object within the specified range. 
    
    """
    delta = end_date - start_date
    time_dict = {
        'days' : rn.randint(0, delta.days),
        'seconds': rn.randint(0, delta.seconds),
        'microseconds': rn.randint(0, 999999)
        }
    
    return start_date + timedelta(**time_dict)

# Example usage:
start_date = datetime(2023, 11, 1, 0, 0, 0)  # Replace with your desired start date
end_date = datetime(2023, 12, 13, 23, 59, 59)  # Replace with your desired end date

random_date = random_datetime(start_date, end_date)
print("Randomly generated datetime:", random_date)

# Delete every document in a collection
past_doc.delete_many({})

# Past orders schema
cus_candidates = list(range(1,21))
past_orders = []

rn.seed(12)
fresh_id_name = products_doc.find({'segment' : 'fresh'}, {'_id' : 1, 'product_name' : 1})
others_id_name = products_doc.find({'segment' : 'others'}, {'_id' : 1, 'product_name' : 1})


# Storing the fresh and other products into separate lists
fresh = []
others = []
for x in fresh_id_name:
    fresh.append(x)

for x in others_id_name:
    others.append(x)
    
boolean = ('fresh', 'others')





# Begin generating instances for past_orders
for i in cus_candidates:
    for j in range(5):
        items_list = []
        k = rn.randint(1,6)
        
        segment = rn.sample(boolean, 1)
        if  segment[0] == 'fresh':
            random_prod = rn.sample(globals()['fresh'], k)
        else:
            random_prod = rn.sample(globals()['others'], k)
        for n in range(k):
            num = rn.randint(1,5)
            item = {
                "product_id":  random_prod[n]['_id'],
                "product_name": random_prod[n]['product_name'],
                "quantity": num
            }
            items_list.append(item)
            
        past_order = {
            "items": items_list,
            "segment": segment[0],
            "total_amount" : 0,
            "customer_id": i,
            "status": 'Delivered',
            "payment_timestamp" : random_datetime(datetime(2023, 11, 1, 0, 0, 0), datetime(2023, 12, 13, 23,59,59))
        }
        past_orders.append(past_order)
        
for x in past_orders:
    past_doc.insert_one(x)




"""
CURRENT ORDER
"""
# Current orders schema
status = ['In Cart', 'Out For Delivery', 'Paid']
cus_candidates = list(range(1,21))
cur_orders = []

# Delete every document in a collection
cur_doc.delete_many({})

# Begin generating instances for current_orders
for i in cus_candidates:
    for j in range(2):
        items_list = []
        k = rn.randint(1,6)
        
        segment = rn.sample(boolean, 1)
        if  segment[0] == 'fresh':
            random_prod = rn.sample(globals()['fresh'], k)
        else:
            random_prod = rn.sample(globals()['others'], k)
            
        for n in range(k):
            num = rn.randint(1,5)
            item = {
                "product_id":  random_prod[n]['_id'],
                "product_name":random_prod[n]['product_name'],
                "quantity": num
            }
            items_list.append(item)
        
            
        cur_order = {
            "items": items_list,
            "segment" : segment[0],
            "total_amount" : 0,
            "customer_id": i,
            "status": rn.sample(status, 1)[0],
            "payment_timestamp" : random_datetime(datetime(2023, 12, 14, 0, 0, 0), datetime(2023, 12, 14, 23,59,59))
        }
        cur_orders.append(cur_order)
        
for x in cur_orders:
    cur_doc.insert_one(x)


# Current order and past order total amount aggregation pipeline
"""
# Main aggregation
db.past_orders.aggregate([
    { $unwind: "$items" },
    {
        $lookup: {
            from: "products",
            localField: "items.product_id",
            foreignField: "_id",
            as: "product_info"
        }
    },
    { $unwind: "$product_info" },
    {
        $set: {
            "items.total_cost": {
                $multiply: ["$items.quantity", "$product_info.standard_price_to_customers"]
            }
        }
    },
    {
        $group: {
            _id: "$_id",
            items: {
                $push: "$items",
            },
            total_amount: {
                $sum: "$items.total_cost"
            },
            customer_id: {
                $first: "$customer_id"
            },
            status: {
                $first: "$status"
            }
        }
    },
    {
      $unset : 'items.total_cost'
     },
    {
        $merge: {
            into: "past_orders",
            whenMatched: "merge"
        }
    }
]);
"""


# Aggregation pipeline to look up the total spend for each customer and find the top 5 spender
"""
db.customers.aggregate(
  [
    {
      $lookup: {
        from: 'past_orders',
        localField: 'customer_id',
        foreignField: 'customer_id',
        as: 'past',
        pipeline: [
          { $project: { total_amount: 1 } }
        ]
      }
    },
    { $unwind: '$past' },
    {
      $lookup: {
        from: 'current_orders',
        localField: 'customer_id',
        foreignField: 'customer_id',
        as: 'current',
        pipeline: [
          {
            $project: {
              status: 1,
              total_amount: 1
            }
          }
        ]
      }
    },
    { $unwind: '$current' },
    {
      $group: {
        _id: '$customer_id',
        total_paid_past: {
          $sum: '$past.total_amount'
        },
        total_paid_current: {
          $sum: {
            $cond: {
              if: {
                $and: [
                  {
                    $ne: [
                      '$current.status',
                      'Out For Delivery'
                    ]
                  },
                  {
                    $ne: [
                      '$current.status',
                      'In Cart'
                    ]
                  }
                ]
              },
              then: '$current.total_amount',
              else: 0
            }
          }
        }
      }
    },
    {
      $set: {
        customer_id: '$_id',
        total_spend: {
          $add: [
            '$total_paid_past',
            '$total_paid_current'
          ]
        }
      }
    },
    {
     $sort:{
         'total_spend':-1
         }
     },
    {
     $limit : 5
     },
    {
      $project: {
        _id: 0,
        customer_id: 1,
        total_spend: 1
      }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);
"""



"""
PRODUCTS
"""
# Products 
lst = []

# Fruits
product1 = {
            'segment' : 'fresh',
            'category' : 'fruits & vegetables',
            'product_name' : 'Box Of Apple',
            'description' : ' A sensory journey that combines elegance, flavor, and the rich heritage of Japanese fruit cultivation. Each bite is a testament to the exceptional quality that defines these sought-after apples.',
            'product_dimension_lwh' : '20 x 20 x 20',
            'product_weight_kg' : 8,
            'product_quantity' : 12,
            'expiry_date_ddmmyyyy' : '18/12/2023',
            'country_of_origin' : 'Japan',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 122,
            'cost_of_products_from_morrizon' : 90
         }
lst.append(product1)

product2 = {
            'segment' : 'fresh',
            'category' : 'fruits & vegetables',
            'product_name' : 'Cucumber',
            'description' : 'Cucumbers have a mild, refreshing taste and a high water content. They can help relieve dehydration and are pleasant to eat in hot weather. People eat cucumber as a savory food, but it is a fruit. It also features in some beauty products. Cucumber also contains a range of B vitamins, vitamin A, and antioxidants, including a type known as lignans. Cucumber also contains 19.9mg of calcium.',
            'product_dimension_lwh' : '2 x 2 x 12',
            'product_weight_kg' : 0.6,
            'product_quantity' : 1,
            'expiry_date_ddmmyyyy' : '15/12/2023',
            'country_of_origin' : 'United Kingdom',
            'customer_rating_score' : 5,
            'standard_price_to_customers' : 0.85,
            'cost_of_products_from_morrizon' : 0.83
            }        
lst.append(product2)   

product3 = {
            'segment' : 'fresh',
            'category' : 'fruits & vegetables',
            'product_name' : 'Cantaloupe',
            'description' : 'With a vibrant orange hue and a tantalizing aroma, our Honey Kiss Cantaloupe promises a sensory journey that begins the moment you lay eyes on it. Each fruit is handpicked at the peak of ripeness, guaranteeing a succulent and flavorful treat that captures the essence of summer.',
            'product_dimension_lwh' : '16 x 13 x 17',
            'product_weight_kg' : 0.9,
            'product_quantity' : 1,
            'expiry_date_ddmmyyyy' : '17/12/2023',
            'country_of_origin' : 'Japan',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 2.79,
            'cost_of_products_from_morrizon' : 2.6
            }        
lst.append(product3) 

product4 = {
            'segment' : 'fresh',
            'category' : 'fruits & vegetables',
            'product_name' : 'Broccoli',
            'description' : 'A rich green color and a dense texture, broccoli boasts a cruciferous charm that enhances both visual appeal and nutritional value. The flavor is earthy and slightly bitter, offering a pleasant balance when cooked to perfection. Broccoli is not only a culinary delight but also a nutrient powerhouse, delivering essential vitamins, minerals, and antioxidants.',
            'product_dimension_lwh' : '10 x 10 x 13',
            'product_weight_kg' : 0.5,
            'product_quantity' : 1,
            'expiry_date_ddmmyyyy' : '15/12/2023',
            'country_of_origin' : 'United Kingdom',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 1.15,
            'cost_of_products_from_morrizon' : 1
            }        
lst.append(product4) 

product5 = {
            'segment' : 'fresh',
            'category' : 'fruits & vegetables',
            'product_name' : 'Pineapple',
            'description' : 'With its spiky crown and rough, hexagonal-patterned skin, is a tropical fruit known for its sweet and tangy flavor. The outer skin is typically a combination of green, yellow, and brown hues. Inside, the fruit features a juicy, fibrous, and golden-yellow flesh. The core is edible but is usually discarded. Pineapples are not only delicious but are also rich in vitamin C and manganese, making them a refreshing and nutritious treat.',
            'product_dimension_lwh' : '10 x 9 x 24',
            'product_weight_kg' : 1.1,
            'product_quantity' : 1,
            'expiry_date_ddmmyyyy' : '15/12/2023',
            'country_of_origin' : 'Indonesia',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 1.99,
            'cost_of_products_from_morrizon' : 1.67
            }        
lst.append(product5) 

# Bakery
product6 = {
            'segment' : 'fresh',
            'category' : 'bakery',
            'product_name' : 'Pain Au Chocolat',
            'description' : 'A pastry that marries the delicate layers of flaky, buttery dough with the rich indulgence of high-quality chocolate.',
            'product_dimension_lwh' : '13 x 9 x 15',
            'product_weight_kg' : 0.4,
            'product_quantity' : 6,
            'expiry_date_ddmmyyyy' : '16/12/2023',
            'country_of_origin' : 'United Kingdom',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 1.99,
            'cost_of_products_from_morrizon' : 1.23
         }
lst.append(product6)

product7 = {
            'segment' : 'fresh',
            'category' : 'bakery',
            'product_name' : 'Red Velvet Cake',
            'description' : 'Our Red Velvet Cake is a masterpiece of culinary artistry, featuring layers of moist, vibrant red cake with a hint of cocoa, generously sandwiched between velvety cream cheese frosting. The exterior is often adorned with elegant swirls of frosting or topped with decorative elements, creating a visually stunning dessert. With a perfect balance of sweetness and subtle cocoa flavor, each slice offers a heavenly experience that captures the essence of this classic indulgence',
            'product_dimension_lwh' : '20 x 20 x 6',
            'product_weight_kg' : 1.7,
            'product_quantity': 1,
            'expiry_date_ddmmyyyy' : '16/12/2023',
            'country_of_origin' : 'United Kingdom',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 8.49,
            'cost_of_products_from_morrizon' : 8.3
         }
lst.append(product7)

product8 = {
            'segment' : 'fresh',
            'category' : 'bakery',
            'product_name' : 'Croissant',
            'description' : 'Crafted with precision and passion, our croissants are a symphony of delicate layers, golden-brown flakiness, and a buttery aroma that fills the air. Each bite is a journey through the artistry of French pastry-making, delivering a perfect balance of crispiness on the outside and a soft, melt-in-your-mouth interior.',
            'product_dimension_lwh' : '14 x 5 x 6',
            'product_weight_kg' : 0.08,
            'product_quantity': 1,
            'expiry_date_ddmmyyyy' : '16/12/2023',
            'country_of_origin' : 'United Kingdom',
            'customer_rating_score' : 5,
            'standard_price_to_customers' : 1.5,
            'cost_of_products_from_morrizon' : 1.0
         }
lst.append(product8)

product9 = {
            'segment' : 'fresh',
            'category' : 'bakery',
            'product_name' : 'Milk Brownies',
            'description' : 'Treat yourself to the perfect union of chocolate and milk, and let the irresistible allure of our Milk Brownies become a staple in your indulgent repertoire',
            'product_dimension_lwh' : '15 x 13 x 11',
            'product_weight_kg' : 0.17,
            'product_quantity': 14,
            'expiry_date_ddmmyyyy' : '16/12/2023',
            'country_of_origin' : 'United Kingdom',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 2.39,
            'cost_of_products_from_morrizon' : 2
         }
lst.append(product9)

product10 = {
            'segment' : 'fresh',
            'category' : 'bakery',
            'product_name' : 'Christmas Cupcakes',
            'description' : 'Treat yourself to the perfect union of chocolate and milk, and let the irresistible allure of our Milk Brownies become a staple in your indulgent repertoire',
            'product_dimension_lwh' : '20 x 20 x 7',
            'product_weight_kg' : 0.9,
            'product_quantity': 9,
            'expiry_date_ddmmyyyy' : '16/12/2023',
            'country_of_origin' : 'United Kingdom',
            'customer_rating_score' : 4,
            'standard_price_to_customers' : 10,
            'cost_of_products_from_morrizon' : 7
         }
lst.append(product10)


for item in lst:
    products_doc.insert_one(item)



# MongoDB cilent close
client.close()



