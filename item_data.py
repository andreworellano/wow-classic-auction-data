import requests as r
import pandas as pd
from authetincation import generate_token
from database import DbConn
import json

# goal of this get request
    # 1 build item dim table
        # this is completed in this file
    # 2 refresh a fact table daily with current auctions
        # this will be done in the auction_data.py file. this file ended up just being a one time load due to the individual API calls needed per item id

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

locale_params = {
    "namespace": "static-classic-us",
    "locale": "en_US"
}

item_api_url = "https://us.api.blizzard.com/data/wow/item/{item_id}"

# api call 

data = r.get(url=f"https://us.api.blizzard.com/data/wow/connected-realm/{connectedRealmId}/auctions/{auctionHouseID}", params=auction_params, headers=header_auth).json()


# move to json file

    # import json

    # with open('auction_data.json', 'w') as json_file:
    #     json.dump(data, json_file)

# dim item table get request in a loop from the blizz api 

item_ids = data["auctions"]

item_id_list = []

for item_id in item_ids:
    item_id_list.append(item_id["item"]["id"])

unique_item_id_list = set(item_id_list)

test_list = [4297, 8392, 14627]

# print(unique_item_id_list)

# grab item information 

# list variables to capture each data point

# item_id_loaded = []
# item_name = []
# item_class = []
# item_class_id = []
# item_subclass = []
# item_subclass_id = []

# ids that errored out
timeout = r.Timeout
timeout_ids = []
keyerror_ids = []

def get_item_info(item_id_list):
    # create a dictionary to capture item data 
    item_dict = {
    # use an array so that each id is it's own index -- scrapping this
    "item": []
    }
    
    for item_id in item_id_list:
        try:
            item_api_url = f"https://us.api.blizzard.com/data/wow/item/{item_id}"
            item_data = r.get(url=item_api_url, params=locale_params, headers=header_auth).json()
            # get each element from the api call
            item_name = item_data["name"]
            item_class = item_data["item_class"]["name"]
            item_class_id = item_data["item_class"]["id"]
            item_subclass = item_data["item_subclass"]["name"]
            item_subclass_id = item_data["item_subclass"]["id"]
            # move each element into the item dictionary under the "item" list -- scraping this in favor of moving data straight into arrays
            item_dict["item"].append(
                {
                    "item_id": item_id,
                    "item_name": item_name,
                    "item_class": item_class,
                    "item_class_id": item_class_id,
                    "item_subclass": item_subclass,
                    "item_subclass_id": item_subclass_id
                }
            )
        

            # move each json item into a list 
            # item_id_loaded.append(item_id)
            # item_name.append(item_data["name"])
            # item_class.append(item_data["item_class"]["name"])
            # item_class_id.append(item_data["item_class"]["id"])
            # item_subclass.append(item_data["item_subclass"]["name"])
            # item_subclass_id.append(item_data["item_subclass"]["id"])
        except timeout:
            timeout_ids.append(item_id)
            print("Id has timed out and been added to list")
        except KeyError:
            keyerror_ids.append(item_id)
            print("keyerror found id added to list")
    return item_dict

  
item_dict_return = get_item_info(unique_item_id_list)

# move to individual arrays

item_id_loaded = []
item_name = []
item_class = []
item_class_id = []
item_subclass = []
item_subclass_id = []

for item in item_dict_return['item']:
    item_id_loaded.append(item["item_id"])
    item_name.append(item["item_name"])
    item_class.append(item["item_class"])
    item_class_id.append(item["item_class_id"])
    item_subclass.append(item["item_subclass"])
    item_subclass_id.append(item["item_subclass_id"])

# move to dict

item_dict = {
    "item_id": item_id_loaded,
    "item_name": item_name,
    "item_class_id": item_class_id,
    "item_class": item_class,
    "item_subclass_id": item_subclass_id,
    "item_subclass": item_subclass
}

# with open(item_dict.json) as h:
#     h.write(json.dumps(item_dict))

# move to pandas df 

item_df = pd.DataFrame(item_dict, columns=["item_id", "item_name", "item_class_id", "item_class", "item_subclass_id", "item_subclass"])

# if a item_name returns null then have the DAG or package add the item_ids for those null values into the list here and kick off this package again

# open db connection

conn = DbConn()
cur = conn.cursor()
print("conn and cur opened")

# load the df into postgres 

def dim_item_insert(cur, item_id, item_name, item_class_id, item_class, item_subclass_id, item_subclass):
    load_sql = "INSERT INTO dim_item VALUES (%s, %s, %s, %s, %s, %s)"
    rows_to_insert = (item_id, item_name, item_class_id, item_class, item_subclass_id, item_subclass)
    cur.execute(load_sql, rows_to_insert)


for i, row in item_df.iterrows():
    dim_item_insert(cur, row['item_id'], row['item_name'], row['item_class_id'], row['item_class'], row['item_subclass_id'], row['item_subclass'])

print(timeout_ids)
print(keyerror_ids)

# timeout ids [3771, 4609, 8279, 10033, 10331, 10404, 11023, 11083, 12037, 13034, 13882, 14243, 15184, 22308, 24589, 25681, 25901, 28497, 31156]
# keyerror ids [14469, 14477, 14480, 14500, 14505]

# with open(timeout_ids.txt, 'w') as f:
#     f.write(json.dumps(timeout_ids))

# with open(keyerror_ids.txt, 'w') as g:
#     g.write(json.dumps(keyerror_ids))

conn.commit()
cur.close()
conn.close()
print("connection closed")

# tomorrow 6/8 truncate current table and try full load 