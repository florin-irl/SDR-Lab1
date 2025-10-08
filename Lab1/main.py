from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.exceptions import APIException
from recombee_api_client.api_requests import *
import random
import pandas as pd
import math
import re
from datetime import datetime

# configurare client

client = RecombeeClient(
  'florin-ivana-dev',
  'q3D7QnkhjuQdcWvb8LL0UScCKkuLjsO4PKwNky3CiCLujjbsK5sHI70PqS7Y0Uy3',
  region=Region.EU_WEST
)

# incarcare fisier dataset

df = pd.read_excel("amazon_prime_dataset_trimmed.xlsx")

# definire proprietati itemi

requests = [
    AddItemProperty("show_type", "string"),  # tip
    AddItemProperty("title", "string"),  # titlu
    AddItemProperty("director", "set"),  # regizor
    AddItemProperty("cast", "set"),  # cast
    AddItemProperty("countries", "set"),  # tara
    AddItemProperty("date_added", "timestamp"),  # date added
    AddItemProperty("release_year", "int"),  # an lansare
    AddItemProperty("content_rating", "string"),  # rating
    AddItemProperty("duration_label", "string"),  # durata brut
    AddItemProperty("duration_minutes", "int"),  # durata
    AddItemProperty("listed_in", "set"),  # gen
    AddItemProperty("description", "string"),
]

client.send(Batch(requests))
print("Prop au fost adaugate.")

# incarcare itemi efectivi

requests = []

for index, row in df.iterrows():
    item_id = str(row["show_id"]) if not pd.isna(row["show_id"]) else f"item_{index}"
    requests.append(AddItem(item_id))

    values = {}

    # campuri directe
    if isinstance(row.get("type"), str):
        values["show_type"] = row["type"].strip()

    if isinstance(row.get("title"), str):
        values["title"] = row["title"].strip()

    if isinstance(row.get("description"), str):
        values["description"] = row["description"].strip()

    # campuri cu seturi de date
    if isinstance(row.get("director"), str):
        values["director"] = [x.strip() for x in row["director"].split(",") if x.strip()]

    if isinstance(row.get("cast"), str):
        values["cast"] = [x.strip() for x in row["cast"].split(",") if x.strip()]

    if isinstance(row.get("country"), str):
        values["countries"] = [x.strip() for x in row["country"].split(",") if x.strip()]

    if isinstance(row.get("listed_in"), str):
        values["listed_in"] = [x.strip() for x in row["listed_in"].split(",") if x.strip()]

    # timestamp
    if pd.notna(row.get("date_added")):
        try:
            ts = pd.to_datetime(row["date_added"], errors="coerce")
            if pd.notna(ts):
                values["date_added"] = int(ts.timestamp())
        except:
            pass

    # numeric properties
    if pd.notna(row.get("release_year")):
        try:
            values["release_year"] = int(row["release_year"])
        except:
            pass

    if isinstance(row.get("rating"), str):
        values["content_rating"] = row["rating"].strip()

    if isinstance(row.get("duration"), str):
        values["duration_label"] = row["duration"].strip()
        match = re.search(r"(\d+)\s*min", row["duration"].lower())
        if match:
            values["duration_minutes"] = int(match.group(1))

    # adaugare in requesturi ce vor fi trimise
    if values:
        requests.append(SetItemValues(item_id, values, cascade_create=True))

# trimitem pe batchuri

CHUNK = 500
for i in range(0, len(requests), CHUNK):
    client.send(Batch(requests[i:i+CHUNK]))
    print(f"Trimis batch {i//CHUNK + 1}")

print(f"Incarcat {len(df)} itemi + proprietați în Recombee.")