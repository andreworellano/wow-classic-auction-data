# World of Warcraft Auction Data Project

This is an application to pull auction data from the Blizzard World of Warcraft API. 

auction_data.py is the only file meant to be run daily limited to only 3 high value items to limit cloud database storage.

The application fed two relational tables in postgres dim_item (PK item_id) and fact_auction (PK auction_id FK item_id). 

The tables were used to analyze daily auction prices for 3 consumables - daily quanity and avg buyout price. 

## License
[MIT](https://choosealicense.com/licenses/mit/)