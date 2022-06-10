from operator import ge
import requests as r
from authetincation import generate_token

# api params

access_token = generate_token()

locale_params = {
    "namespace": "dynamic-classic-us",
    "locale": "en_US"
}

header_auth = {
    "Authorization": f"Bearer {access_token}"
}

connectedRealmId = 4384

# api call

data = r.get(url=f"https://us.api.blizzard.com/data/wow/connected-realm/{connectedRealmId}/auctions/index", params=locale_params, headers=header_auth).json()

# print data

# returns auction house ids

print(data)