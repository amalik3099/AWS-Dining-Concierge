import json
import datetime
import time
import os
import dateutil.parser
import logging
import boto3
import re

import urllib3


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Constants
SQS_CLIENT = boto3.client('sqs')
QueueUrl   = 'https://sqs.us-east-1.amazonaws.com/614741084428/Q1'

VALID_LOCATIONS = ["Sutton Place, NY", "Rockefeller Center, NY", "Diamond District, NY", "Theater District, NY", 
                    "Washington Heights, NY", "Hudson Heights, NY", "West Harlem, NY", "Hamilton Heights, NY", "Manhattanville, NY", "Morningside Heights, NY", "Central Harlem, NY",
                    "Turtle Bay, NY", "Midtown East, NY", "Midtown, NY", "Tudor City, NY", "Little Brazil, NY", "Times Square, NY", "Hudson Yards, NY", "Bronx, NY", "Flushing, NY",
                    "Midtown West, NY", "Hell's Kitchen, NY", "Garment District, NY", "Herald Square, NY", "Koreatown, NY", "Murray Hill, NY", "Tenderloin, NY", "Bayside, NY",
                    "Madison Square, NY", "Flower District, NY", "Brookdale, NY", "Hudson Yards, NY", "Kips Bay, NY", "Rose Hill, NY", "NoMad, NY", "Peter Cooper Village, NY",
                    "Chelsea, NY", "Flatiron District, NY", "Gramercy Park, NY", "Stuyvesant Square, NY", "Union Square, NY", "Stuyvesant Town, NY", "Meatpacking District, NY",
                    "Harlem, NY", "St. Nicholas Historic District, NY", "Astor Row, NY", "Marcus Garvey Park, NY", "Le Petit Senegal, NY", "East Harlem, NY",
                    "Waterside Plaza, NY", "Downtown Manhattan, NY", "Little Germany, NY", "Alphabet City, NY", "East Village, NY", "Greenwich Village, NY", "NoHo, NY",
                    "Bowery, NY", "West Village, NY", "Lower East Side	, NY", "SoHo, NY", "Nolita, NY", "Little Australia, NY", "Little Italy, NY", "Chinatown	, NY",
                    "Financial District, NY", "Five Points, NY", "Cooperative Village, NY", "Two Bridges, NY", "Tribeca, NY", "Civic Center, NY", "Radio Row , NY",
                    "South Street Seaport, NY", "Battery Park City, NY", "Little Syria, NY", "Upper Manhattan, NY", "Marble Hill, NY", "Inwood, NY", "Fort Georg, NY",
                    "Brooklyn Heights, NY", "Dumbo, NY", "Williamsburg, NY", "Greenpoint, NY", "Cobble Hill, NY", "Sunnyside, NY", "Astoria, NY", "Woodside, NY",
                    "Upper East Side, NY", "Lenox Hill, NY", "Carnegie Hill, NY", "Yorkville, NY", "Upper West Side, NY", "Manhattan Valley, NY", "Lincoln Square, NY"]

VALID_AREAS = ["New York City", "New York", "NYC"] #, "Manhattan", "Queens", "Bronx", "Staten Island", "Brooklyn"]

VALID_CUISINES =  ['Indian', 'Thai', 'Mexican', 'Chinese', 'Italian', 'Japanese', 'Korean']

# --- Helpers that build all of the responses ---

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def send_sqs_message(slots):
    # Send message to SQS queue

    MessageBody = f"Food Recommendations for {slots['Email']}"

    MessageAttributes={
        'NoOfPeople': {
            'DataType': 'String',
            'StringValue': slots["NumberOfPeople"]
        },
        'Date': {
            'DataType': 'String',
            'StringValue': slots["DiningDate"]
        },
        'Time': {
            'DataType': 'String',
            'StringValue': slots["DiningTime"]
        },
        'PhoneNumber' : {
            'DataType': 'String',
            'StringValue': slots["PhoneNumber"]
        },
        'Cuisine': {
            'DataType': 'String',
            'StringValue': slots["Cuisine"]
        },
        'Email' : {
            'DataType': 'String',
            'StringValue': slots["Email"]
        },
        'Location' : {
            'DataType': 'String',
            'StringValue': slots["Location"]
        }
    }

    response = SQS_CLIENT.send_message(
        QueueUrl            =   QueueUrl,
        MessageAttributes   =   MessageAttributes,
        MessageBody         =   MessageBody
        )

    logger.debug(f"SQS Message Response {response['MessageId']}")

    return response['MessageId']

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    print("Eliciting Slot Now")
    print(slot_to_elicit, message)
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


# def confirm_intent(session_attributes, intent_name, slots, message):
#     return {
#         'sessionAttributes': session_attributes,
#         'dialogAction': {
#             'type': 'ConfirmIntent',
#             'intentName': intent_name,
#             'slots': slots,
#             'message': message
#         }
#     }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

    return response


# --- Helper Functions ---

def get_lower_strings(arr):
    return [ x.lower() for x in arr]

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

# def safe_int(n):
#     """
#     Safely convert n value to int.
#     """
#     if n is not None:
#         return int(n)
#     return n


# def try_ex(func):
#     """
#     Call passed in function in try block. If KeyError is encountered return None.
#     This function is intended to be used to safely access dictionary.

#     Note that this function would have negative impact on performance.
#     """

#     try:
#         return func()
#     except KeyError:
#         return None



def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        response = {
            'isValid': is_valid,
            'violatedSlot': violated_slot
        }

        return response
    
    response = {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {
            'contentType': 'PlainText',
            'content': message_content
        }
    }

    return response
###########
TABLENAME = 'yelp-restaurants'
USER_TABLENAME = 'user-data'
OPENSEARCH_ENDPOINT = "https://search-dining-search-lmfrtycvvmxbzenypmtnse4heq.us-east-1.es.amazonaws.com/"
REGION = "us-east-1"

master_username = 'master'
master_password = 'PeterParker#2022'

db_resource = boto3.resource('dynamodb', region_name=REGION)

def get_userdata(email, db=None, table=USER_TABLENAME):
    if not db:
        db = db_resource
    table = db.Table(table)
    res = table.get_item(Key={'email': email})
    if 'Item' in res.keys():
        return res['Item']
    return None

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
        

        
#########
def is_valid_cuisine(cuisine):
    if cuisine.lower() not in get_lower_strings(VALID_CUISINES):
        return False
    return True
    
def is_valid_number_of_people(number_of_people):
    number_of_people = parse_int(number_of_people)
    if number_of_people > 20 or number_of_people < 1:
        return False
    return True
        
def is_valid_date(dining_date):
    # Check if the date is in the past
    print("Dining Date: ", dining_date)
    if datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
        return False
    # Check if date is more than one week into the future
    elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() > (datetime.datetime.now() + datetime.timedelta(days=7)).date():
        return False
    return True

def is_valid_time(dining_date, dining_time):
    if datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() == datetime.date.today():
        if datetime.datetime.strptime(dining_time, '%H:%M').time() <= datetime.datetime.now().time():
            return False
    return True

def is_valid_location(location):
    print(location.lower() not in get_lower_strings(VALID_LOCATIONS))
    print(location.lower() not in get_lower_strings(VALID_AREAS))
    if location.lower() not in get_lower_strings(VALID_LOCATIONS) and location.lower() not in get_lower_strings(VALID_AREAS) :
        return False
    return True

def validate_dining_suggestions_intent(location, cuisine, number_of_people, dining_date, dining_time, phone_number, email):

    # if email is not None:
    #     VALIDATE_EMAIL = True
    #     user_data = get_userdata(email)
    #     print("Checking DB")
    #     if user_data is not None:
    #         print("The user data is present")
    #         print(user_data)
    #         return build_validation_result(False, 'Location', f"Based on our last conversation, you looked for {user_data['cuisine']} recommendations in {user_data['location']}, for {user_data['number_of_people']} people. What location are you looking for today?")
    
    if location is not None:
        if not is_valid_location(location):
            return build_validation_result(False, 'Location', f'We are not functional in {location} at the moment. Please choose from ' + ', '.join(VALID_AREAS) + '.')

    if cuisine is not None:
        if not is_valid_cuisine(cuisine):
            return build_validation_result(False, 'Cuisine', 'Hmmm. We do not serve that cuisine yet. Maybe try choosing from ' + ', '.join(VALID_CUISINES) + '.')
    
    if number_of_people is not None:
        if not is_valid_number_of_people(number_of_people):
            if parse_int(number_of_people) < 1:
                return build_validation_result(False, 'NumberOfPeople', 'I am sorry but I cannot make a recommendation for a party of less than 1.')
            return build_validation_result(False, 'NumberOfPeople', 'I am sorry I the maximum number of people in allowed in a party is 20.')
            
    if dining_date is not None:
        if not is_valid_date(dining_date):
            return build_validation_result(False, 'DiningDate', 'Please enter a correct date. You cannot select a date that has already passed or one week from today.')
    
    if dining_time is not None and dining_date is not None:
        if not is_valid_time(dining_date, dining_time):
            return build_validation_result(False, 'DiningTime', 'Please enter valid time.')

    if phone_number is not None:
        print("Phone Number Check: ", re.match(r"/^(\([0-9]{3}\) ?-?|[0-9]{3}-?)[0-9]{3}-?[0-9]{4}$/gm", phone_number))
        if re.match(r"^(\([0-9]{3}\)[ -]?|[0-9]{3}-?)[0-9]{3}-?[0-9]{4}$", phone_number) is None:
            return build_validation_result(False, 'PhoneNumber', 'Please enter a valid US Phone number')
        
        # Check if the number is present in the DynamoDB table
        # Check if the user wants to reuse previous recommendation => add slot yes/no
        # If yes 
            # Load all slots with information from DynamoDB
            
    return build_validation_result(True, None, None)





def greeting_intent(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help?'}
        }
    }

def thankyou_intent(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You are welcome!'}
        }
    }

def dining_suggestions(intent_request):
    location = get_slots(intent_request)["Location"]
    previous_user = get_slots(intent_request)["PreviousUser"]
    cuisine = get_slots(intent_request)["Cuisine"]
    number_of_people = get_slots(intent_request)["NumberOfPeople"]
    dining_date = get_slots(intent_request)["DiningDate"]
    dining_time = get_slots(intent_request)["DiningTime"]
    phone_number = get_slots(intent_request)["PhoneNumber"]
    email = get_slots(intent_request)["Email"]

    source = intent_request["invocationSource"]
    
    
    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)

        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        validation_result = validate_dining_suggestions_intent(location, cuisine, number_of_people, dining_date, dining_time, phone_number, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

        is_email_checked = 'email_validated' in intent_request['sessionAttributes'].keys()
            
        if email is not None and is_email_checked == False:
            intent_request['sessionAttributes']['email_validated'] = 'email_validated'
            print("Entered Here Once")
            output_session_attributes['email_validated'] = True
            user_data = get_userdata(email)
            if user_data is not None:
                prev_cuisine = user_data['cuisine'] + ' restaurants'
                new_res_ids = poll_opensearch(prev_cuisine)
                new_queries = poll_dynamo(new_res_ids)
            
                output_session_attributes['cuisine'] = user_data['cuisine']
                output_session_attributes['location'] = user_data['location']
                output_session_attributes['number_of_people'] = user_data['number_of_people']
                output_session_attributes['rec1'] = new_queries[0]['name']
                output_session_attributes['rec2'] = new_queries[1]['name']
                output_session_attributes['rec3'] = new_queries[2]['name']
                output_session_attributes['rec4'] = new_queries[3]['name']
                output_session_attributes['rec5'] = new_queries[4]['name']
                
                print("Checking for email. Eliciting PreviousUser")
                return elicit_slot(intent_request['sessionAttributes'],
                              intent_request['currentIntent']['name'],
                              slots,
                              'PreviousUser',
                              {'contentType': 'PlainText', 'content': f"Based on our last conversation, you looked for {user_data['cuisine']} recommendations in {user_data['location']}, for {user_data['number_of_people']} people. We recommended {new_queries[0]['name']}, {new_queries[1]['name']}, {new_queries[2]['name']}, {new_queries[3]['name']}, and {new_queries[4]['name']}. Would you like to go ahead with these suggestions? (Please enter \"yes\" or \"no\")"})
            else:
                return elicit_slot(intent_request['sessionAttributes'],
                              intent_request['currentIntent']['name'],
                              slots,
                              'Location',
                              {'contentType': 'PlainText', 'content': f"Great! What city or city area are you looking to dine in today?"})
                
        if previous_user is not None:
            print("#"*10)
            print(previous_user)
            print(previous_user.lower() == "yes")
            print("#"*10)
            if previous_user == "yes":
                user_data = get_userdata(email)
                
                send_slots = {} 
                send_slots["NumberOfPeople"] = user_data['number_of_people']
                send_slots["DiningDate"] = user_data['dining_date']
                send_slots["DiningTime"] = user_data['dining_time']
                send_slots["PhoneNumber"] = "1234567890"
                send_slots["Cuisine"] = user_data['cuisine']
                send_slots["Location"] = user_data['location']
                send_slots["Email"] = user_data['email']

                
                send_sqs_message(send_slots)
                
                return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': f'You\'re all set. You will get a set of recommendations based on your past history!'})
            else:
                pass
        

                
        return delegate(output_session_attributes, get_slots(intent_request))
            
    # Once we Enter the Fullfillment Hook we send the SQS message
    send_sqs_message(get_slots(intent_request))
    

    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': f'You\'re all set. I will look for restaurants serving {cuisine} in {location} for {number_of_people} people at {dining_time} on {dining_date}. Expect my suggestions shortly! Have a good day.'})
    

# --- Intents ---


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)
    elif intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'ThankyouIntent':
        return thankyou_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    
    print("event", event)
    print("context:", context)

    return dispatch(event)