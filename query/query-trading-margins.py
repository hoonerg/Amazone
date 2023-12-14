#importing
import pymongo
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

#Connect to MongoDB
conn_str = "mongodb+srv://julius:julius@amazone.4sbahbn.mongodb.net/"
client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
db = client['amazone']
product_collection = db.products

#Creating pipeline
pipeline = [
    {
        "$match": {
            "cost_to_Amazone": {"$exists": True}
        }
    },
    {
        "$project": {
            "product_name": 1,
            "category": 1,
            "segment": 1,  # Include 'segment' in the projection
            "profit_margin": {
                "$subtract": ["$standard_price_to_customers", "$cost_to_Amazone"]
            }
        }
    }
]

#Running Query
product_profit = product_collection.aggregate(pipeline)

#Creating DataFrames
df = pd.DataFrame(product_profit)
segments = df['segment'].unique()

#Creating Graphs
for segment in segments:
    segment_df = df[df['segment'] == segment]
    segment_df = segment_df.sort_values(by='profit_margin', ascending=True)

    plt.figure(figsize=(12, 8))
    categories = segment_df['category'].unique()
    colors = plt.cm.get_cmap('tab20', len(categories))

    for i, category in enumerate(categories):
        subset = segment_df[segment_df['category'] == category]
        plt.bar(subset['product_name'], subset['profit_margin'], color=colors(i), label=category)

    plt.ylabel('Trading Margin per Unit in GBP')
    plt.xlabel('Product')
    plt.title(f'Trading Margin per Unit by Product in Segment: {segment}')
    plt.legend(title='Product Category')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()
