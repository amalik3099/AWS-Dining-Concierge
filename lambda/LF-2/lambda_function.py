import json
import boto3
import requests
import urllib3
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

TABLENAME = 'yelp-restaurants'
USER_TABLENAME = 'user-data'
OPENSEARCH_ENDPOINT = "https://search-dining-search-lmfrtycvvmxbzenypmtnse4heq.us-east-1.es.amazonaws.com/"
SQS_ENDPOINT = "https://sqs.us-east-1.amazonaws.com/614741084428/Q1"
REGION = "us-east-1"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   REGION, 'es', session_token=credentials.token)
master_username = 'master'
master_password = 'PeterParker#2022'

sqs_client = boto3.client('sqs', region_name=REGION)
db_resource = boto3.resource('dynamodb', region_name=REGION)
ses_client = boto3.client('ses', region_name=REGION)


# lookup item in DynamoDB by ID
def poll_dynamo(ids_list, db=None, table=TABLENAME):
    if not db:
        db = db_resource
    table = db.Table(table)
    res_info = []
    for r_id in ids_list:
        res = table.get_item(Key={'id': str(r_id)})
        if 'Item' in res:
            res_info.append(res['Item'])
    return res_info


def poll_opensearch(cuisine):
    http = urllib3.PoolManager()
    url = OPENSEARCH_ENDPOINT + 'restaurants/_search'
    headers = urllib3.make_headers(basic_auth='%s:%s' % (master_username, master_password))
    headers.update({
        'Content-Type': 'application/json',
        "Accept": "application/json"
    })
    query = {
              "query": {
                  "query_string": {
                        "query": cuisine,
                        'type': 'cross_fields',
                    }
                }
            }
    res = http.request('GET', url, headers=headers, body=json.dumps(query))
    status = res.status
    json_res = json.loads(res.data)
    hit_list = json_res["hits"]["hits"]
    id_list = []
    short_list = []
    for hit in hit_list:
        r_id = hit["_source"]["id"]
        id_list.append(r_id)
    if len(id_list) > 5:
        return id_list[:5]


def unpack_message(message):
    data = {
        'cuisine': message['MessageAttributes']['Cuisine']['StringValue'],
        'location': message['MessageAttributes']['Location']['StringValue'],
        'email': message['MessageAttributes']['Email']['StringValue'],
        'dining_date': message['MessageAttributes']['Date']['StringValue'],
        'dining_time': message['MessageAttributes']['Time']['StringValue'],
        'number_of_people': message['MessageAttributes']['NoOfPeople']['StringValue'],
    }
    return data
    
    
def recieve_sqs_message():
    sqs = sqs_client
    queue_url = SQS_ENDPOINT
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=5,
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=20
        )
    return response
    
    
def delete_sqs_message(message):
    sqs = sqs_client
    queue_url = SQS_ENDPOINT
    sqs.delete_message(QueueUrl=queue_url,
    ReceiptHandle=message['ReceiptHandle']
    )
    print("Message Deleted")


def send_html_email(data, query, user_data=None, user_query=None):
    CHARSET = "UTF-8"
    if user_data: 
        HTML_EMAIL_CONTENT = f"""
            <html>
                <h1 id="food-recommendations">Food Recommendations</h1>
                <p>Hello!</p>
                <p>Thank you for using our services. Here are my {data['cuisine']} recommendations for {data['number_of_people']} people, at {data['dining_time']} on {data['dining_date']},</p>
                <ul>
                <li>{query[0]['name']}, located at {query[0]['address']}, {query[0]['locale']} {query[0]['zip_code']} with a rating of {query[0]['rating']}</li>
                <li>{query[1]['name']}, located at {query[1]['address']}, {query[1]['locale']} {query[1]['zip_code']} with a rating of {query[1]['rating']}</li>
                <li>{query[2]['name']}, located at {query[2]['address']}, {query[2]['locale']} {query[2]['zip_code']} with a rating of {query[2]['rating']}</li>
                <li>{query[3]['name']}, located at {query[3]['address']}, {query[3]['locale']} {query[3]['zip_code']} with a rating of {query[3]['rating']}</li>
                <li>{query[4]['name']}, located at {query[4]['address']}, {query[4]['locale']} {query[4]['zip_code']} with a rating of {query[4]['rating']}</li>
                </ul>
                <br>
                <p>Based on our last conversation, you looked for {user_data['cuisine']} recommendations in {user_data['location']}, for {user_data['number_of_people']} people</p>
                <p>Here are some more {user_data['cuisine']} recommendations that we think you will like!</p>
                <ul>
                <li>{user_query[0]['name']}, located at {user_query[0]['address']}, {user_query[0]['locale']} {user_query[0]['zip_code']} with a rating of {user_query[0]['rating']}</li>
                <li>{user_query[1]['name']}, located at {user_query[1]['address']}, {user_query[1]['locale']} {user_query[1]['zip_code']} with a rating of {user_query[1]['rating']}</li>
                <li>{user_query[2]['name']}, located at {user_query[2]['address']}, {user_query[2]['locale']} {user_query[2]['zip_code']} with a rating of {user_query[2]['rating']}</li>
                <li>{user_query[3]['name']}, located at {user_query[3]['address']}, {user_query[3]['locale']} {user_query[3]['zip_code']} with a rating of {user_query[3]['rating']}</li>
                <li>{user_query[4]['name']}, located at {user_query[4]['address']}, {user_query[4]['locale']} {user_query[4]['zip_code']} with a rating of {user_query[4]['rating']}</li>
                </ul>
                <p>Have a nice day!</p>
            </html>
        """
    else:
        HTML_EMAIL_CONTENT = f"""
            <html>
                <h1 id="food-recommendations">Food Recommendations</h1>
                <p>Hello!</p>
                <p>Thank you for using our services. Here are my {data['cuisine']} recommendations for {data['number_of_people']} people, at {data['dining_time']} on {data['dining_date']},</p>
                <ul>
                <li>{query[0]['name']}, located at {query[0]['address']}, {query[0]['locale']} {query[0]['zip_code']} with a rating of {query[0]['rating']}</li>
                <li>{query[1]['name']}, located at {query[1]['address']}, {query[1]['locale']} {query[1]['zip_code']} with a rating of {query[1]['rating']}</li>
                <li>{query[2]['name']}, located at {query[2]['address']}, {query[2]['locale']} {query[2]['zip_code']} with a rating of {query[2]['rating']}</li>
                <li>{query[3]['name']}, located at {query[3]['address']}, {query[3]['locale']} {query[3]['zip_code']} with a rating of {query[3]['rating']}</li>
                <li>{query[4]['name']}, located at {query[4]['address']}, {query[4]['locale']} {query[4]['zip_code']} with a rating of {query[4]['rating']}</li>
                </ul>
                <p>Have a nice day!</p>
            </html>
        """

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                data['email'],
            ],
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": CHARSET,
                    "Data": HTML_EMAIL_CONTENT,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Food Recommendations",
            },
        },
        Source="ooctavius2022@gmail.com",
    )


# push user data to user-data dynamodb table
def push_dynamo(data, db=None, table=USER_TABLENAME):
    if not db:
        db = db_resource
    table = db.Table(table)
    data_arr = {
        'email': data['email'],
        'location': data['location'],
        'cuisine': data['cuisine'],
        'dining_date': data['dining_date'],
        'dining_time': data['dining_time'],
        'number_of_people': data['number_of_people']
    }
    response = table.put_item(Item=data_arr)
    return response


def get_userdata(email, db=None, table=USER_TABLENAME):
    if not db:
        db = db_resource
    table = db.Table(table)
    res = table.get_item(Key={'email': email})
    if 'Item' in res.keys():
        return res['Item']
    return None


def lambda_handler(event, context):
    sqsQueueResponse = recieve_sqs_message()
    print(sqsQueueResponse)
    
    if "Messages" in sqsQueueResponse.keys():
        for message in sqsQueueResponse['Messages']:
            data = unpack_message(message)
            cuisine = data['cuisine'] + ' restaurants'
            
            # getting query from open search
            restaurant_ids = poll_opensearch(cuisine)
            
            # getting query from dynamodb 
            queries = poll_dynamo(restaurant_ids)
            
            # check if user exists in user-data table 
            user_data = get_userdata(data['email'])
            if user_data:
                prev_cuisine = user_data['cuisine'] + ' restaurants'
                new_res_ids = poll_opensearch(prev_cuisine)
                new_queries = poll_dynamo(new_res_ids)
                # send email with prev user data and new suggestions
                send_html_email(data, queries, user_data, new_queries)
            else: 
                # send email with new suggestions
                send_html_email(data, queries)
            
            # push user data to user-data dynamodb table
            res = push_dynamo(data)
            
            # delete message from queue
            delete_sqs_message(message)

