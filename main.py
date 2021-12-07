import ssl
import paho.mqtt.client as mqtt
from pymongo import MongoClient


def get_database():
    CONNECTION_STRING = "mongodb+srv://admin:admin@webiot.dkgkv.mongodb.net/WebIOT?retryWrites=true&w=majority"

    client = MongoClient(CONNECTION_STRING)

    return client['WebIOT']


steps_db = get_database()

steps_collection = steps_db['Steps']


def on_connect(client, userdata, flags, rc):
    print("Connected with result code {0}".format(str(rc)))
    client.subscribe("crosswalk")


def on_message(client, userdata, msg):
    if msg.topic == 'crosswalk' and msg.payload.decode('utf-8') == 'crossed':

        test = {
            'client_name': 'semaphore',
            'steps': 10
        }

        steps_query = {'client_name': 'semaphore'}
        result = steps_collection.find(steps_query)

        if any(s['client_name'] == 'semaphore' for s in result):
            result.rewind()
            steps_dict = result.next()
            steps_dict['steps'] = int(steps_dict['steps']) + 10
            steps_collection.update_one(steps_query, {"$set": steps_dict})
            client.publish('crosswalk', payload='updated', qos=0, retain=False)
            print('updated!')

        else:
            steps_collection.insert_one(test)
            print('inserted!')


mqtt_client = mqtt.Client("python")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect('localhost')
mqtt_client.loop_forever()
