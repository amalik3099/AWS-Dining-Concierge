from tqdm import tqdm
import boto3
import json
import csv
import pandas as pd
from datetime import datetime
from session_details import get_locations, get_cuisines, search_yelp


def poll_data():
    outfile = 'data.json'
    data = []
    loc_list = get_locations()
    cuisine_list = get_cuisines()
    for location in loc_list:
        for cuisine in cuisine_list:
            yelp_reply = search_yelp(cuisine, location)
            for reply in yelp_reply:
                reply['cuisine'] = cuisine
                reply['locale'] = location
            data += yelp_reply

        with open(outfile, 'a+', encoding='utf-8') as f:
            for reply in data:
                json_record = json.dumps(reply, ensure_ascii=False)
                f.write(json_record + '\n')


def clean_json():
    seen = []
    count = 0
    with open('clean_data.json', 'a+', encoding='utf-8') as g:
        with open('data.json', encoding='utf-8') as f:
            for line in f:
                data_point = json.loads(line)
                res_id = data_point.get('id', '')
                if res_id not in seen:
                    seen.append(res_id)
                    count = count + 1
                    g.write(line)
    print(count)            


# convert data from json to csv to store in DynamoDB
def convert_json():
    with open('clean_data.json', encoding='utf-8') as f:
        with open('clean_data.csv', 'w+', newline='') as f_csv:
            r_csv = csv.writer(f_csv, delimiter=',')
            for line in f:
                data_point = json.loads(line)
                row = [data_point.get('id', ''), data_point.get('name', ''), data_point.get('location', '')['address1'],
                        data_point.get('locale', ''),
                        data_point.get('coordinates', '')['latitude'], data_point.get('coordinates', '')['longitude'], 
                        data_point.get('review_count', ''), data_point.get('rating', ''), 
                        data_point.get('location', '')['zip_code'], data_point.get('cuisine', ''), 
                        data_point.get('phone', ''), data_point.get('price', ''), 
                        data_point.get('image_url', ''), data_point.get('url', '')]
                
                r_csv.writerow(row)


def load_dynamo_data():
    db = boto3.resource('dynamodb')
    table = db.Table("yelp-restaurants")
    req_attrs = ['id', 'name', 'address', 'locale', 'latitude', 'longitude', 'review_count', 'rating', 'zip_code',
                'cuisine', 'phone', 'price', 'image_url', 'url']

    i = 1
    with open('clean_data.csv', 'r', newline='') as f_csv:
        r_csv = csv.reader(f_csv, delimiter=',')

        with tqdm(total=5600) as tracker:
            for row in r_csv:
                dp = {req_attrs[i]: row[i] for i in range(1, len(req_attrs))}

                dp['id'] = row[0]
                dp['insertedAtTimestamp'] = str(datetime.now())

                table.put_item(Item=dp)
                # track progress of upload
                tracker.update(1)
                i += 1
                if i > 5600:
                    break


# format data.json to push to elastic search
# def format_json():
#     seen = set()
#     with open('elastic.json', 'a+', encoding='utf-8') as g:
#         with open('clean_data.json', encoding='utf-8') as f:
#             for line in f:
#                 if line not in seen: seen.add(line)
#                 data_point = json.loads(line)
#                 res_id = data_point.get('id', '')
#                 res_cuisine = data_point.get('cuisine', '')
#                 g.write('{"index": {"_index": "restaurants", "_type": "Restaurant"}}' + '\n')
#                 g.write('{"id": "' + res_id + '", "cuisine": "' + res_cuisine + '"}' + '\n')

def format_json():
    sorted_arr = []
    with open('clean_data.json', encoding='utf-8') as f:
        for dp in f:
            data_p = json.loads(dp)
            sorted_arr.append(data_p)
    sorted_arr.sort(key=lambda x: x["review_count"], reverse=True)
    with open('elastic_sort.json', 'a+', encoding='utf-8') as g:
        for data_point in sorted_arr:
            res_id = data_point.get('id', '')
            res_cuisine = data_point.get('cuisine', '')
            # print(data_point.get('review_count', ''))
            g.write('{"index": {"_index": "restaurants", "_type": "Restaurant"}}' + '\n')
            g.write('{"id": "' + res_id + '", "cuisine": "' + res_cuisine + '"}' + '\n')


if __name__ == "__main__":
    #scrape yelp data with yelp API
    poll_data()
    # clean json data and remove duplicates
    clean_json()
    # convert json to csv
    convert_json()
    # push csv data to DynamoDB
    load_dynamo_data()
    # format clean json data to push to Open Search
    format_json()

