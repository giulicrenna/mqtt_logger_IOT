import paho.mqtt.client as mqtt
from datetime import datetime
from datetime import date
import csv
import json

HOST = "mqtt.darkflow.com.ar"
PORT = 1883
topic = "DeviceData/2448230"

filename = "data-" + str(date.today()) + ".csv"

def createCSV():
    try:    
        with open(filename, 'r') as file:
            writer = csv.reader(file, delimiter = ';')
            for row in writer:
                if row == ["timestamp", "temperature", "humidity"]:
                    break;
                else:
                    with open(filename, 'w') as file:
                        writer_ = csv.writer(file, delimiter = ';')
                        writer_.writerow(["timestamp", "temperature", "humidity"])
                        break;
    except FileNotFoundError:
        with open(filename, 'w') as file:
            writer_ = csv.writer(file, delimiter = ';')
            writer_.writerow(["timestamp", "temperature", "humidity"])
        
def on_message(client, userdata, message):
    with open(filename, 'a') as file:
        writer = csv.writer(file, delimiter = ';')
        try:
            msg = json.loads(message.payload.decode("utf-8"))
            timestamp = datetime.fromtimestamp(int(msg["Timestamp"]))
            temperature = msg["Value"][0]["Value"]
            humidity = msg["Value"][1]["Value"]
            print(str(json.loads(message.payload.decode("utf-8"))))
            writer.writerow([timestamp,
                         temperature,
                         humidity
                        ])
        except UnicodeDecodeError:
            pass

mqttc = mqtt.Client()
mqttc.connect(host=HOST, port=PORT, keepalive=100)
mqttc.on_message = on_message

def task():
    print("READING FROM " + topic)
    mqttc.subscribe(topic, qos=0)
    while True:
        mqttc.loop_start()
    


if __name__ == '__main__':
    createCSV()
    task()