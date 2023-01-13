import paho.mqtt.client as mqtt
from datetime import datetime
from datetime import date
import csv
import json
import os

HOST = "mqtt.darkflow.com.ar"
PORT = 1883
topic = "DeviceData/2448230"

flag_connected = False
lastDay = date.today()

def on_connect(client, userdata, flags, rc):
    global flag_connected
    flag_connected = True

def on_disconnect(client, userdata, rc):
    global flag_connected
    flag_connected = False

def createCSV():
    try:
        with open(filename, 'r', newline='') as file:
            writer = csv.reader(file, delimiter=',')
            for row in writer:
                if row == ["timestamp", "temperature", "humidity"]:
                    break
                else:
                    with open(filename, 'w', newline='') as file:
                        writer_ = csv.writer(file, delimiter=',')
                        writer_.writerow(
                            ["timestamp", "temperature", "humidity"])
                        break
    except FileNotFoundError:
        with open(filename, 'w', newline='') as file:
            writer_ = csv.writer(file, delimiter=',')
            writer_.writerow(["timestamp", "temperature", "humidity"])

def on_message(client, userdata, message):
    try:
        currentDay = date.today()
        if(lastDay == currentDay):
            if flag_connected:
                with open(filename, 'a', newline='') as file:
                    writer = csv.writer(file, delimiter=',')
                    try:
                        try:
                            msg = json.loads(message.payload.decode("utf-8"))
                            timestamp = datetime.fromtimestamp(int(msg["Timestamp"]))
                            # "\"" +  msg["Value"][0]["Value"] + "\""
                            temperature = msg["Value"][0]["Value"]
                            # "\"" + msg["Value"][1]["Value"] + "\""
                            humidity = msg["Value"][1]["Value"]
                        except json.decoder.JSONDecodeError:
                            pass
                        print(str(json.loads(message.payload.decode("utf-8"))))
                        writer.writerow([str(timestamp),
                                        temperature,
                                        humidity
                                        ])
                    except UnicodeDecodeError:
                        pass
            elif not(flag_connected):
                print("Trying to reconnect...")
                task()
    except PermissionError:
        pass

mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect

def task():
    os.system('cls')
    print("Reading from " + topic)
    print("Writing to: " + filename)
    mqttc.connect(host=HOST, port=PORT, keepalive=100)
    mqttc.subscribe(topic, qos=0)
    while True:
        currentDay = date.today()
        if lastDay == currentDay:
            mqttc.loop()
        else:
            startTask()
            
def startTask():
    global lastDay
    global filename
    lastDay = date.today()
    filename = "data-" + str(lastDay) + ".csv"
    createCSV()
    task()

if __name__ == '__main__':
    startTask()
