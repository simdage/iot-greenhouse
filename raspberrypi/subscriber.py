import json
import os
from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

# MQTT configuration
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "greenhouse/+/sensors")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))

# InfluxDB configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Initialize InfluxDB Client
influx_client = None
write_api = None

if all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET]):
    try:
        influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        print("Initialized InfluxDB Client successfully.")
    except Exception as e:
        print(f"Error initializing InfluxDB client: {e}")
else:
    print("WARNING: InfluxDB environment variables are incomplete. Running in log-only mode.")

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback for when the client connects to the broker."""
    if reason_code == 0:
        print(f"Successfully connected to MQTT Broker ({MQTT_BROKER}:{MQTT_PORT})")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Connection failed with reason code: {reason_code}")

def on_message(client, userdata, msg):
    """Callback for when a message is received from the broker."""
    try:
        # Parse topic to get node ID (e.g. "greenhouse/node01/sensors" -> "node01")
        topic_parts = msg.topic.split('/')
        node_id = topic_parts[1] if len(topic_parts) >= 2 else "unknown"

        # Parse the JSON payload
        payload = json.loads(msg.payload.decode("utf-8"))
        ts_str = payload.get("ts")
        temp = payload.get("temp")
        hum = payload.get("hum")
        pres = payload.get("pres")
        rssi = payload.get("rssi")

        print(f"\n[{ts_str or 'No Timestamp'}] Telemetry from {node_id}:")
        print(f"  Temperature : {temp:.2f} °C")
        print(f"  Humidity    : {hum:.2f} %")
        print(f"  Pressure    : {pres:.2f} hPa")
        print(f"  RSSI        : {rssi} dBm")

        # Ingest into InfluxDB
        if write_api:
            # Parse timestamp to UTC datetime
            if ts_str:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            else:
                dt = datetime.utcnow()

            point = Point("greenhouse_sensors") \
                .tag("node", node_id) \
                .field("temperature", float(temp)) \
                .field("humidity", float(hum)) \
                .field("pressure", float(pres)) \
                .field("rssi", int(rssi)) \
                .time(dt, WritePrecision.NS)

            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            print("  Ingested into InfluxDB.")

    except json.JSONDecodeError:
        print(f"Failed to parse JSON payload: {msg.payload}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    # Initialize the client using CallbackAPIVersion.VERSION2 for compatibility with paho-mqtt v2.x
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    print("Connecting to broker...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"Could not connect to broker: {e}")
        return

    # Start loop to process incoming network traffic and callbacks
    print("Starting MQTT subscriber loop. Press Ctrl+C to exit.")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nExiting subscriber...")
        if influx_client:
            influx_client.close()
        client.disconnect()

if __name__ == "__main__":
    main()
