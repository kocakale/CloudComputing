import datetime
import json
import os

import boto3
import requests
from pathlib import Path

for i in range (15,22):

    # Set DATE_PARAM to the desired date
    DATE_PARAM = "2023-10-"+str(i)
    # Create a date object from the DATE_PARAM
    date = datetime.datetime.strptime(DATE_PARAM, "%Y-%m-%d")

    # Wikimedia API URL formation
    # See https://wikimedia.org/api/rest_v1/#/Metrics%20Pageviews%20Top


    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{date.strftime('%Y/%m/%d')}?page=1&per_page=1000"
    print(f"Requesting REST API URL: {url}")

    # Getting response from Wikimedia API
    wiki_server_response = requests.get(url, 
                                        headers={"User-Agent": "curl/7.68.0"})
    wiki_response_status = wiki_server_response.status_code
    wiki_response_body = wiki_server_response.text

    print(f"Wikipedia REST API Response body: {wiki_response_body}")
    print(f"Wikipedia REST API Response Code: {wiki_response_status}")

    # Create a local directory for saving raw views data
    if not os.path.exists("raw-views"):
        os.makedirs("raw-views")

    # Save the raw API response to a file named raw-views-YYYY-MM-DD
    raw_views_file = f"raw-views/{date.strftime('%Y-%m-%d')}"
    with open(raw_views_file, "w") as file:
        file.write(wiki_response_body)
    
    # Upload the raw file to S3
    s3 = boto3.client("s3")
    s3.upload_file(raw_views_file, "ceu-yahya-wikidata", 
                   f"datalake/raw/raw-views-{date.strftime('%Y-%m-%d')}.txt")
    
    # Convert the JSON string into a dictionary
    pageviews_data = json.loads(wiki_response_body)
    
    # Convert the response into a JSON lines formatted file
    date_utc = datetime.datetime.utcfromtimestamp(int(date.strftime("%Y%m%d")))
    
    json_lines = ""
    
    for article in pageviews_data['items'][0]['articles']:
        record = {"article": article["article"],
                  "views": int(article["views"]),
                  "rank": article["rank"],
                  "date": date.strftime("%Y-%m-%d"),
                  "retrieved_at": datetime.datetime.utcnow().isoformat(),
                  }
        json_lines += json.dumps(record) + "\n"

    ## Get the directory of the current file
    current_directory = Path(__file__).parent

    # Path for the new directory
    JSON_LOCATION_BASE = current_directory / "data" / "views"

    # Create the new directory, ignore if it already exists
    JSON_LOCATION_BASE.mkdir(exist_ok=True, parents=True)
    print(f"Created directory {JSON_LOCATION_BASE}")

    # Save the JSON lines file to your computer to data/views/views-YYYY-MM-DD.json
    views_file = f"data/views/{date.strftime('%Y-%m-%d')}.json"
    with open(views_file, "wb") as file:
        file.write(json_lines.encode("utf-8"))

    # Upload the JSON lines file to S3
    s3.upload_file(views_file, "ceu-yahya-wikidata", 
                   f"datalake/views/views-{date.strftime('%Y-%m-%d')}.txt")