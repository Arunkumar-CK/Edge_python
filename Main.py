"""
This program read the data from cloud and checks for duplicate copies in mongo DB.
Then adds the status field for each messages and stores in mongo DB    

"""

import time
import json
import pymongo
from azure.iot.device import IoTHubDeviceClient

RECEIVED_MESSAGES = 0
CONNECTION_STRING = "HostName=carteplus-iothub-nred.azure-devices.net;DeviceId=ckplcnred;SharedAccessKey=yE+SgpaLcuXFlNRy745eX/1pSOiw4jGro4sDCRoj/Tc="
MONGODB_URI = "mongodb://127.0.0.1:27017/"
DATABASE_NAME = "database"
COLLECTION_NAME = "table"


def message_handler(message):
    global RECEIVED_MESSAGES
    RECEIVED_MESSAGES += 1
    # print data from both system and application (custom) properties
    #for property in vars(message).items():
    #  print ("    {}".format(property))
    message_data = message.data.decode('utf-8') 
    data_json = json.loads(message_data) 
    data_json['status'] = "Received from cloud"
    #print(json.dumps(data_json, indent=4))  
    
    try:
       
      client = pymongo.MongoClient(MONGODB_URI)
      database = client[DATABASE_NAME]
      collection = database[COLLECTION_NAME]
      if data_json['commandType']<=6:
        if 'pRequestId' in data_json:
         query = {'pRequestId': data_json['pRequestId']}
         result = collection.find_one(query)
         if not result:
            collection.insert_one(data_json) 
            print("prequest")
                     
        elif 'rRequestId' in data_json:
            query = {'rRequestId': data_json['rRequestId']}
            result = collection.find_one(query)
            if not result:
             collection.insert_one(data_json)
             print("rrequest")
        else :
          collection.insert_one(data_json)
          print("else")

    except pymongo.errors.ConnectionFailure:
        print("Failed to connect to MongoDB.")
 

def main():
    print ("Starting the Python IoT Hub C2D Messaging device")
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    print ("Waiting for C2D messages, press Ctrl-C to exit")
    try:
     
        client.on_message_received = message_handler
        
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        print("IoT Hub C2D Messaging device stopped")
    

if __name__ == '__main__':
    main()    

    