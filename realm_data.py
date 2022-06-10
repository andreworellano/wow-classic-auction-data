import requests
import json
import pandas as pd
import psycopg2
from config import config
from authetincation import generate_token
from database import DbConn

# one time script to load data into realm table

# blizzard api realm list
request_url = 'https://us.api.blizzard.com/data/wow/realm/index'

# hard coded access token a function is created to get this that will be built into a function
access_token = generate_token()

# params needed to get from realm api
params_param = {
    "namespace": "dynamic-classic-us",
    "locale": "en_US"
}

# auth header
header_auth = {
    "Authorization": f"Bearer {access_token}"
}

# request from realm api
r = requests.get(request_url, params=params_param, headers=header_auth)
print(r)
data = r.json()
# print(r_json["realms"])

# realm data arrays
realm_names = []
realm_id = []

for realm in data["realms"]:
    realm_names.append(realm["name"])
    realm_id.append(realm["id"])

realm_dict = {
    "realm_name": realm_names,
    "realm_id": realm_id
}

realm_df = pd.DataFrame(realm_dict, columns=["realm_name", "realm_id"])
# print(realm_df)

# open db
conn = DbConn()
# open cursor
cur = conn.cursor()
# insert realm data func
def insert_realm_data(cur, realm_id, realm_name):
    sql = ("""INSERT INTO realm (realm_id, realm_name) VALUES (%s, %s);
""")
    row_to_insert = (realm_id, realm_name)
    cur.execute(sql, row_to_insert)
# insert data
for i, row in realm_df.iterrows():
    insert_realm_data(cur, row['realm_id'], row['realm_name'] )

# commit
conn.commit()
# close
conn.close()
cur.close()
print("DB Connection Closed")
