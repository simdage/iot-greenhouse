import os
import json
import collections
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
ALERT_TOPIC = "greenhouse/alerts/anomaly"

# InfluxDB configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Initialize InfluxDB Client (for bootstrapping history and logging alerts)
influx_client = None
query_api = None
write_api = None

if all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET]):
    try:
        influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        query_api = influx_client.query_api()
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        print("Initialized InfluxDB Client successfully for history bootstrapping and alert logging.")
    except Exception as e:
        print(f"Error initializing InfluxDB client: {e}")
else:
    print("WARNING: InfluxDB configuration is incomplete. Skipping history bootstrap and alert logging.")

# Sliding windows configuration
WINDOW_SIZE = 30
# Dictionary structure: { node_id: { metric_name: deque([val1, val2, ...]) } }
data_windows = collections.defaultdict(lambda: {
    "temp": collections.deque(maxlen=WINDOW_SIZE),
    "hum": collections.deque(maxlen=WINDOW_SIZE),
    "pres": collections.deque(maxlen=WINDOW_SIZE),
    "rssi": collections.deque(maxlen=WINDOW_SIZE)
})

def calculate_stats(window):
    """Calculates mean and standard deviation of a sequence in pure Python."""
    n = len(window)
    if n == 0:
        return 0.0, 0.0
    mean = sum(window) / n
    variance = sum((x - mean) ** 2 for x in window) / n
    std = variance ** 0.5
    return mean, std

def detect_anomaly(node_id, metric, current_value, window, threshold=3.0):
    """Detects anomalies using a simple rolling z-score method."""
    if len(window) < 10:  # Require a minimal baseline history size
        return False, 0.0, 0.0
    
    mean, std = calculate_stats(window)
    if std > 0.01:  # Check if there is enough variance to compute Z-score
        z_score = abs(current_value - mean) / std
        if z_score > threshold:
            return True, mean, std
            
    return False, mean, std

def bootstrap_history():
    """Bootstraps the rolling window history from InfluxDB upon startup."""
    if not query_api:
        print("No InfluxDB query API available. Running without bootstrapping history.")
        return
    
    print("Bootstrapping sliding window history from InfluxDB...")
    # Retrieve the last 1 hour of data to populate the sliding windows
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r["_measurement"] == "greenhouse_sensors")
      |> filter(fn: (r) => r["_field"] == "temperature" or r["_field"] == "humidity" or r["_field"] == "pressure" or r["_field"] == "rssi")
      |> keep(columns: ["_time", "node", "_field", "_value"])
      |> sort(columns: ["_time"])
    '''
    try:
        tables = query_api.query(query)
        field_mapping = {
            "temperature": "temp",
            "humidity": "hum",
            "pressure": "pres",
            "rssi": "rssi"
        }
        count = 0
        for table in tables:
            for record in table.records:
                node = record.values.get("node")
                field = record.get_field()
                val = record.get_value()
                metric = field_mapping.get(field)
                if node and metric and val is not None:
                    data_windows[node][metric].append(float(val))
                    count += 1
        print(f"Successfully bootstrapped {count} records from InfluxDB.")
        for node, metrics in data_windows.items():
            loaded_info = ", ".join([f"{m}: {len(win)}" for m, win in metrics.items()])
            print(f"  Node {node}: {loaded_info}")
    except Exception as e:
        print(f"Error bootstrapping history: {e}")

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
        temp = payload.get("temp")
        hum = payload.get("hum")
        pres = payload.get("pres")
        rssi = payload.get("rssi")

        metrics_to_check = {
            "temp": ("Temperature", temp, "°C"),
            "hum": ("Humidity", hum, "%"),
            "pres": ("Pressure", pres, "hPa"),
            "rssi": ("RSSI", rssi, "dBm")
        }

        for metric_key, (metric_name, value, unit) in metrics_to_check.items():
            if value is not None:
                val_float = float(value)
                window = data_windows[node_id][metric_key]
                
                # Check for anomaly using the historical window *before* adding the current value
                is_anomaly, mean, std = detect_anomaly(node_id, metric_key, val_float, window)
                
                # Append the new value to the sliding window
                window.append(val_float)
                
                if is_anomaly:
                    alert_payload = {
                        "ts": datetime.utcnow().isoformat() + "Z",
                        "node_id": node_id,
                        "metric": metric_name,
                        "value": val_float,
                        "mean": round(mean, 2),
                        "std": round(std, 2),
                        "unit": unit,
                        "message": f"Anomaly detected on {node_id}! {metric_name} {val_float}{unit} deviates significantly from mean {mean:.2f}{unit} (std: {std:.2f})."
                    }
                    print(f"\n[ALERT] {alert_payload['message']}")
                    client.publish(ALERT_TOPIC, json.dumps(alert_payload))

                    # Also write the alert to InfluxDB in a separate table/measurement
                    if write_api:
                        try:
                            point = Point("greenhouse_alerts") \
                                .tag("node", node_id) \
                                .tag("metric", metric_name) \
                                .field("value", val_float) \
                                .field("mean", float(mean)) \
                                .field("std", float(std)) \
                                .field("message", alert_payload["message"]) \
                                .time(datetime.utcnow(), WritePrecision.NS)
                            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
                            print("  Alert written to InfluxDB table 'greenhouse_alerts'.")
                        except Exception as write_err:
                            print(f"  Error writing alert to InfluxDB: {write_err}")

    except json.JSONDecodeError:
        print(f"Failed to parse JSON payload from MQTT: {msg.payload}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    # Bootstrap historical values from InfluxDB
    bootstrap_history()

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
    print("Starting MQTT anomaly detector loop. Press Ctrl+C to exit.")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nExiting anomaly detector...")
        if influx_client:
            influx_client.close()
        client.disconnect()

if __name__ == "__main__":
    main()
