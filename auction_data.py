import requests as r
import pandas as pd
from authetincation import generate_token
from database import DbConn
import json

# daily load of auction data

# grab auth token

access_token = generate_token()

# api call paramas

# mankirk
connectedRealmId = 4384
# horde auction house
auctionHouseID = 6

auction_params = {
    # "region": "us",
    # "connectedRealmId": 4384,
    # "auctionHouseId": 2,
    "namespace": "dynamic-classic-us",
    "locale": "en_US"
}

header_auth = {
    "Authorization": f"Bearer {access_token}"
}

# get request

data = r.get(url=f"https://us.api.blizzard.com/data/wow/connected-realm/{connectedRealmId}/auctions/{auctionHouseID}", params=auction_params, headers=header_auth).json()

# move json data into individual arrays

auction_id = []
item_id = []
buyout_amount = []
quantity = []
time_left = []

for row in data["auctions"]:
    auction_id.append(row["id"])
    item_id.append(row["item"]["id"])
    buyout_amount.append(row["buyout"])
    quantity.append(row["quantity"])
    time_left.append(row["time_left"])


# create dict for df

auction_dict = {
    "auction_id": auction_id,
    "item_id": item_id,
    "buyout_amount": buyout_amount,
    "quantity": quantity,
    "time_left": time_left
}

# move data into in pandas dataframe 

auction_df = pd.DataFrame(auction_dict, columns=["auction_id", "item_id", "buyout_amount", "quantity", "time_left"])

test_auction = auction_df.head()

consumable_df = auction_df[(auction_df["item_id"] == 22825) | (auction_df["item_id"] == 22838) | (auction_df["item_id"] == 22839)].drop_duplicates()


# open db connection + cursor
conn = DbConn()
cur = conn.cursor()

# truncate current auction data table -- for this project since it's just practice I want to limit the scope
# i dont want to introduce updates to the database just yet

truncate_fact_auction = "TRUNCATE TABLE fact_auction"
cur.execute(truncate_fact_auction)

# function insert data into fact_auction 

def insert_fact_auction(cur, auction_id, item_id, buyout_amount, quantity, time_left):
    insert_sql = "INSERT INTO fact_auction VALUES (%s, %s, %s, %s, %s)"
    insert_values = (auction_id, item_id, buyout_amount, quantity, time_left)
    cur.execute(insert_sql, insert_values)

# for loop to add in data to database 

for i, row in consumable_df.iterrows():
    insert_fact_auction(cur, row["auction_id"], row["item_id"], row["buyout_amount"], row["quantity"], row["time_left"])

# commit and close db connection

conn.commit()
cur.close()
conn.close()