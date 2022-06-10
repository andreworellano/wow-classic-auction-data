import requests as r
from authetincation import generate_token
import pandas as pd
from database import DbConn

# api params

access_token = generate_token()

locale_params = {
    "namespace": "static-classic-us",
    "locale": "en_US"
}

header_auth = {
    "Authorization": f"Bearer {access_token}"
}

# api call

data = r.get(url="https://us.api.blizzard.com/data/wow/item-class/index", params=locale_params, headers=header_auth).json()


# print(data)

# move data into arrays

item_class_name = []
item_class_id = []

for item_class in data["item_classes"]:
    item_class_name.append(item_class['name'])
    item_class_id.append(item_class['id'])

# move data into dict

item_class_dict = {
    "item_class_name": item_class_name,
    "item_class_id": item_class_id
}

# move dict into df 

item_class_df = pd.DataFrame(item_class_dict, columns=["item_class_id", "item_class_name"])

print(item_class_df)

# load into postgres 

# db con
conn = DbConn()

#db cur
cur = conn.cursor()

def insert_to_table(cur, item_class_id, item_class_name):
    sql = ("""INSERT INTO item_class (item_class_id, item_class_name) VALUES (%s, %s)""")
    rows_to_insert = (item_class_id, item_class_name)
    cur.execute(sql, rows_to_insert)

for i, row in item_class_df.iterrows():
    insert_to_table(cur, row['item_class_id'], row['item_class_name'])

conn.commit()
conn.close()
cur.close()
print("conn closed")
