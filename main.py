import os
import requests
from datetime import *
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


data = []
flight_number = []
origin_airport = []
destination_airport = []
destination = []
departure_at = []
arrival_at = []
airline = []
transfers = []
duration = []
days_before_departure = []
weekday = []
timedate_departure = []
timedate_arrival = []
price = []


origin = "MOW"
destination_parameter = ""
currency = "rub"
departure_at_parameter = "2025-11"
year = "2025"
month = "11"
direct = "true"
group_by = "departure_at"


# DATABASE_URL = os.getenv('DATABASE_URL')
# AVIA_KEY = os.getenv('AVIA_KEY')
DATABASE_URL="postgresql://postgres.jtnvkollorwxeuaiobxp:q.nDe2n4-!BRbF2@aws-1-eu-north-1.pooler.supabase.com:6543/postgres"
AVIA_KEY="a60f8ad1351c2c95edfd9767cd8261c5"
engine = create_engine(DATABASE_URL)
url = f"https://api.travelpayouts.com/aviasales/v3/grouped_prices?origin={origin}&destination={destination_parameter}&currency={currency}&departure_at={year}-{month}&direct={direct}&group_by={group_by}&token={AVIA_KEY}"

# dict_result = requests.get(url).json()
#
# import json
# with open("raw_data.json", "w") as fp:
#     json.dump(dict_result, fp)
#
# print(dict_result['data'])
# for s in (dict_result['data']):
#     print(s)
flights_directions = ['UUS']


def define_timedate(d):
    if(type(d) == str):
        a = datetime.strptime(d, "%Y-%m-%dT%H:%M:%S%z")
    else:
        a = d
    h = a.hour
    if(h > 3):
        if(h < 7):
            return "Early morning"
        elif(h < 12):
            return "Morning"
        elif(h < 17):
            return "Afternoon"
        elif(h < 19):
            return "Early evening"
        else:
            return "Evening"
    else:
        return "Night"


for direction1 in flights_directions:
    print(direction1)
    # going through each month flights pricing
    for i in range(11, 11 + 12):
        if(i < 13):
            month = str(i)
        else:
            i1 = i - 12
            if(i1 < 10):
                month = "0" + str(i1)
            else:
                month = str(i1)
            year = "2026"

        urls_list = [
            f"https://api.travelpayouts.com/aviasales/v3/grouped_prices?origin={origin}&destination={direction1}&currency={currency}&departure_at={year}-{month}&direct={direct}&group_by={group_by}&token={AVIA_KEY}",
            f"https://api.travelpayouts.com/aviasales/v3/grouped_prices?origin={origin}&destination={direction1}&currency={currency}&departure_at={year}-{month}&group_by={group_by}&token={AVIA_KEY}"
        ]

        # url = f"https://api.travelpayouts.com/aviasales/v3/grouped_prices?origin={origin}&destination={direction1}&currency={currency}&departure_at={year}-{month}&direct={direct}&group_by={group_by}&token=a60f8ad1351c2c95edfd9767cd8261c5"

        for url in urls_list:
            # extracting dates
            current_result = requests.get(url).json()
            try:
                data1 = list(current_result['data'])
            except:
                print(f"no flights for {direction1}")
                break

            # extracting data by each date

            for date in data1:
                a = current_result['data'][date]
                flight_number.append(f"{a['airline']}-{a['flight_number']}")
                origin_airport.append(a['origin_airport'])
                destination_airport.append(a['destination_airport'])
                destination.append(a['destination'])
                departure_at.append(a['departure_at'])
                arrival_at.append((datetime.strptime(a['departure_at'], "%Y-%m-%dT%H:%M:%S%z") + timedelta(minutes = int(a['duration']))).strftime("%Y-%m-%dT%H:%M:%S%z"))
                airline.append(a['airline'])
                transfers.append(a['transfers'])
                duration.append(a['duration'])
                days_before_departure.append((datetime.strptime(a['departure_at'], "%Y-%m-%dT%H:%M:%S%z") - datetime.now(timezone.utc)).days)
                weekday.append(datetime.strptime(a['departure_at'], "%Y-%m-%dT%H:%M:%S%z").weekday())
                price.append(a['price'])
                timedate_departure.append(define_timedate(a['departure_at']))
                timedate_arrival.append(define_timedate(datetime.strptime(a['departure_at'], "%Y-%m-%dT%H:%M:%S%z") + timedelta(minutes = int(a['duration']))))


df = pd.DataFrame({"flight_number" : flight_number,
                    "origin_airport": origin_airport,
                   "destination_airport" : destination_airport,
                  "destination" : destination,
                  "departure_at" : departure_at,
                   "arrival_at" : arrival_at,
                  "airline" : airline,
                  "transfers" : transfers,
                  "duration" : duration,
                  "days_before_departure" : days_before_departure,
                   "weekday" : weekday,
                   "departure_day_part" : timedate_departure,
                   "arrival_day_part" : timedate_arrival,
                  "price" : price})
# print(df)

# df.to_excel('dataset.xlsx', index = True)
df.to_sql("flights_data", engine, if_exists="append", index = False)


