import paho.mqtt.client as mqtt
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import influx_config
import mqtt_config
from time import sleep
import logging
import os

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=log_level,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

def celsius_to_fahrenheit(celsius):
    return round((celsius * 9.0/5.0) + 32.0,2)


# mqtt info
mqtt_username = mqtt_config.mqtt_username
mqtt_password = mqtt_config.mqtt_password
mqtt_broker_ip = mqtt_config.mqtt_broker_ip


# set topics to subscribe to
# mqtt_topic = "test"
temperature_topic = "temperature_degC/#"
humidity_topic = "humidity_pct/#"
pressure_topic = "pressure_hpa/#"
raw_moisture_topic = "raw_moisture_sensor/#"

# influx info
bucket = influx_config.bucket
org =  influx_config.org 
token = influx_config.INFLUX_TOKEN
url = influx_config.url


# set up mqtt client
logging.info("Setting up MQTT client...")
client = mqtt.Client()
# Set the username and password for the MQTT client
client.username_pw_set(mqtt_username, mqtt_password)

# wait 5 seconds to give influxdB time to start up. TODO: make this somehow poll until the influx is ready
logging.info("Waiting 5s InfluxDB to start...")
sleep(5)

# set up influxdB client
write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = write_client.write_api(write_options=SYNCHRONOUS)

# These functions handle what happens when the MQTT client connects
# to the broker, and what happens then the topic receives a message
def on_connect(client, userdata, flags, rc):
    # rc is the error code returned when connecting to the broker
    logging.info(f"Connected! {str(rc)}")

    # Once the client has connected to the broker, subscribe to the topic
    # client.subscribe(mqtt_topic)
    client.subscribe(temperature_topic)
    client.subscribe(humidity_topic)
    client.subscribe(pressure_topic)
    client.subscribe(raw_moisture_topic)
    
def on_message(client, userdata, msg):
    # This function is called everytime the topic is published to.
    # If you want to check each message, and do something depending on
    # the content, the code to do this should be run in this function
    try:
        topic_parts = msg.topic.split('/')  # Split the topic into its components
        if len(topic_parts) == 2:
            sensor_type = topic_parts[0]
            sensor_name = topic_parts[1]
            if sensor_type == 'temperature_degC':
                topic_parts = msg.topic.split('/')  # Split the topic into its components
                temperature_degF = celsius_to_fahrenheit(float(msg.payload.decode('utf-8')))
                logging.info(f'Temperature for {sensor_name}: {temperature_degF}°F')
                point = (
                    Point('room_temp')
                    .tag("location", sensor_name)
                    .field(sensor_name + '_temp_degF', temperature_degF)
                )
                write_api.write(bucket=bucket, org=org, record=point)
            elif sensor_type == 'humidity_pct':
                humidity_pct = float(msg.payload.decode('utf-8'))
                logging.info(f'Humidity for {sensor_name}: {humidity_pct}%')
                point = (
                    Point('room_humidity')
                    .tag("location", sensor_name)
                    .field(sensor_name + '_humidity_pct', humidity_pct)
                )
                write_api.write(bucket=bucket, org=org, record=point)
            elif sensor_type == 'pressure_hpa':
                pressure_hpa = float(msg.payload.decode('utf-8'))
                logging.info(f'Pressure for {sensor_name}: {pressure_hpa}hPa')
                point = (
                    Point('room_pressure')
                    .tag("location", sensor_name)
                    .field(sensor_name + '_pressure_hpa', pressure_hpa)
                )
                write_api.write(bucket=bucket, org=org, record=point)
            elif sensor_type == 'raw_moisture_sensor':
                raw_moisture_reading = int(msg.payload.decode('utf-8'))
                logging.info(f'Raw moisture reading for {sensor_name}: {raw_moisture_reading}')
                point = (
                    Point('plant_raw_moisture_reading')
                    .tag("location", sensor_name)
                    .field(sensor_name + '_raw_moisture_reading', raw_moisture_reading)
                )
                write_api.write(bucket=bucket, org=org, record=point)

        else:
            logging.warning(f"Unexpected topic format: {msg.topic}")
    except ValueError as e:
        logging.error(f'Error details: {e}')




# Here, we are telling the client which functions are to be run
# on connecting, and on receiving a message
client.on_connect = on_connect
client.on_message = on_message

# Once everything has been set up, we can (finally) connect to the broker
# 1883 is the listener port that the MQTT broker is using
client.connect(mqtt_broker_ip, 1883)

# Once we have told the client to connect, let the client object run itself
client.loop_forever()
client.disconnect()
