# %%
from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
client = MongoClient('mongodb+srv://roger:roger@amazone.4sbahbn.mongodb.net/')
db = client['amazone']
daily_inventory_collection = db['daily_inventory']
# %%
# The first query retrives the total inventory of each product on the latest day and the second latest day
# Get the latest inventory for each product
pipeline_inventory = [
    # Find the two latest dates
    {
        '$group': {
            '_id': None,  
            'latest_dates': {'$max': '$date'} 
        }
    },
    {
        '$project': {
        'second_latest_date': {
            '$dateSubtract': {
                'startDate': '$latest_dates',
                'unit': 'day',
                'amount': 1
            }
        },
        'latest_date': '$latest_dates'
        }
    },
    # Perform a self-lookup to get the inventory for the two latest dates
    {
        '$lookup': {
            'from': 'daily_inventory',
            'let': {
                'latest_date': '$latest_date', 
                'second_latest_date': '$second_latest_date'
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$or': [
                                {'$eq': ['$date', '$$latest_date']},
                                {'$eq': ['$date', '$$second_latest_date']}
                            ]
                        }
                    }
                }
            ],
            'as': 'selected_inventory'
        }
    },
    # Unwind the array
    {'$unwind': '$selected_inventory'},
    # Replace the root with the new root
    {'$replaceRoot': {'newRoot': '$selected_inventory'}},
    #Another unwind sequence
    {'$unwind': '$warehouses'},
    {
        '$lookup': {
            'from': 'products',
            'localField': 'product_id',
            'foreignField': '_id',
            'as': 'product_details'
        }
    },
    {'$unwind': '$product_details'},
    # Group by product and get their total inventory across all warehouses
    {
        '$group': {
            '_id': {
                'product_name': '$product_details.product_name',
                'segment': '$product_details.segment',
                'category': '$product_details.category',
                'date': '$date'
            },
            'total_inventory': {'$sum': '$warehouses.quantity'}
        }
    }
]

inventory_data = list(daily_inventory_collection.aggregate(pipeline_inventory))

# Create a dataframe for the latest inventory data for each product
data_list = []
for data in inventory_data:
    data_list.append({
        'Product Name': data['_id']['product_name'],
        'Segment': data['_id']['segment'],
        'Category': data['_id']['category'],
        'Date': data['_id']['date'],
        'Total Inventory': data['total_inventory']
    })

df_inventory = pd.DataFrame(data_list)

# Convert 'Date' to datetime so we can filter by it
df_inventory['Date'] = pd.to_datetime(df_inventory['Date'])
df_inventory.sort_values(by = 'Date', inplace = True)

df_fresh_1 = df_inventory[(df_inventory['Segment'] == 'fresh') & (df_inventory['Date'] == pd.Timestamp('2023-12-13'))]
df_fresh_2 = df_inventory[(df_inventory['Segment'] == 'fresh') & (df_inventory['Date'] == pd.Timestamp('2023-12-14'))]
df_others_1 = df_inventory[(df_inventory['Segment'] == 'others') & (df_inventory['Date'] == pd.Timestamp('2023-12-13'))]
df_others_2 = df_inventory[(df_inventory['Segment'] == 'others') & (df_inventory['Date'] == pd.Timestamp('2023-12-14'))]

print("Latest Total Inventory of All Products:")
print(df_inventory)
# %%
# Let's plot the two categories and colour-code the segments within each

def plot_inventory(df, segment_name, date, label_font_size, legend_loc = 'best'):
    df.sort_values(by = ['Category', 'Product Name'], inplace=True)
    categories = df['Category'].unique()

    # Assign a color to each category
    colors = plt.cm.viridis(np.linspace(0, 1, len(categories)))
    category_colors = dict(zip(categories, colors))

    # Create the plot
    plt.figure(figsize = (15, 16))
    for category, color in category_colors.items():
        df_by_category = df[df['Category'] == category]
        plt.bar(df_by_category['Product Name'], df_by_category['Total Inventory'], color = color, label = category)

    plt.xlabel('Product Name', fontsize = label_font_size)
    plt.ylabel('Total Inventory', fontsize = label_font_size)

    # Set the y interval to 500
    max_inventory = df['Total Inventory'].max()
    plt.yticks(np.arange(0, max_inventory + 1, 500), fontsize = 16)

    plt.title(f'Total Inventory by Product: {segment_name} Segment for {date}', fontsize = 26)
    plt.xticks(rotation = 90, fontsize = 16)
    legend = plt.legend(title = 'Category', fontsize = 16, loc = legend_loc, facecolor = 'white', framealpha = 1)
    
    legend.get_title().set_fontsize(18) 
    #plt.legend(title = 'Category', fontsize = 16).get_title().set_fontsize(18) 
    plt.tight_layout()
    plt.show()

# Plot for each segment
plot_inventory(df_fresh_1, 'Fresh', '2023-12-13', 20,)
plot_inventory(df_fresh_2, 'Fresh', '2023-12-14', 20,)
plot_inventory(df_others_1, 'Others', '2023-12-13', 20, legend_loc = 'lower left')
plot_inventory(df_others_2, 'Others', '2023-12-14', 20, legend_loc = 'lower left')
# %%
# The query below retrives daily sales by segment and category. We can create two plots, one for each segment, 
# and plot sales grouped by category over time.

# Pipeline to get daily sales (by segment and category) sorted by date
past_orders_collection = db['past_orders']
pipeline_sales = [
    {
        '$unwind': '$items'
    },
    {
        '$lookup': {
            'from': 'products',
            'localField': 'items.product_id',
            'foreignField': '_id',
            'as': 'product_details'
        }
    },
    {
        '$unwind': '$product_details'
    },
    # Group total sales of items grouped by category within each segment
    {
        '$group': {
            '_id': {
                'segment': '$segment',
                'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$payment_timestamp'}},
                'category': '$product_details.category'
            },  
            'total_sales': {'$sum': '$total_amount'}
        }
    },
    {
        '$sort': {'_id.date': 1}
    }
]

past_orders_data = list(past_orders_collection.aggregate(pipeline_sales))

# Step 2: Convert to DataFrame
data_list = []
for data in past_orders_data:
    data_list.append({
        'Segment': data['_id']['segment'],
        'Date': data['_id']['date'],
        'Total Sales': data['total_sales'],
        'Category': data['_id']['category']
    })

df_sales = pd.DataFrame(data_list)

# Convert 'Date' to datetime
df_sales['Date'] = pd.to_datetime(df_sales['Date'])
df_sales.sort_values(by = 'Date', inplace = True)

# Separate data for 'fresh' and 'others'
df_fresh_sales = df_sales[df_sales['Segment'] == 'fresh']
df_others_sales = df_sales[df_sales['Segment'] == 'others']
# %%
def plot_sales(df, segment):

    plt.figure(figsize=(12, 8))

    for category, group in df.groupby('Category'):
        plt.plot(group['Date'], group['Total Sales'], label = category)

    plt.title(f'Total Sales for {segment} Segment')
    plt.xlabel('Date')
    plt.ylabel('Total Sales')

    date_range = pd.date_range(df['Date'].min(), df['Date'].max(), freq='7D')
    date_labels = date_range.strftime('%Y-%m-%d')

    # Set x-axis 
    plt.xticks(date_range, date_labels)
    plt.xticks(rotation = 45)
    plt.legend(title = 'Category')
    plt.tight_layout()
    plt.show()

# Plot for each segment
plot_sales(df_fresh_sales, 'Fresh')
plot_sales(df_others_sales, 'Others')
client.close()