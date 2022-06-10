import requests as r
from authetincation import generate_token
import pandas as pd
from database import DbConn

# only interested in consumable data id = 0 

# params 
access_token = generate_token()

locale_params = {
    "namespace": "static-classic-us",
    "locale": "en_US"
}

header_auth = {
    "Authorization": f"Bearer {access_token}"
}

itemClassId = 0

# api call 

data = r.get(url=f"https://us.api.blizzard.com/data/wow/item-class/{itemClassId}", params=locale_params, headers=header_auth).json()

# looks like this grabbing additional types --> consumes --> food & drink etc

# move data to arrays

item_type_id = []
item_type_name = []

for item_type in data['item_subclasses']:
    item_type_id.append(item_type['id'])
    item_type_name.append(item_type['name'])

item_type_dict = {
    "item_type_id": item_type_id,
    "item_type_name": item_type_name
}

item_type_df = pd.DataFrame(item_type_dict, columns=("item_type_id", "item_type_name"))
print(item_type_df)

# db connection

conn = DbConn()
cur = conn.cursor()

# grab item_class id

cur.execute(f"SELECT id FROM item_class WHERE item_class_id = {itemClassId}")
item_class_num = cur.fetchone()

# load data to postgres

def insert_item_type_data(cur, item_class_id, item_type_id, item_type_name):
    sql = ("""INSERT INTO item_type (item_class_id, item_type_id, item_type_name) VALUES (%s, %s, %s)""")
    rows_to_insert = (item_class_id, item_type_id, item_type_name)
    cur.execute(sql, rows_to_insert)

for index, row in item_type_df.iterrows():
    insert_item_type_data(cur, item_class_num, row['item_type_id'], row['item_type_name'])

# commit and close
conn.commit()
conn.close()
cur.close()