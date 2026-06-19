#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>

#include "secrets.h"

const char* ssid        = SECRET_SSID;
const char* password    = SECRET_PASS;
const char* mqtt_server = SECRET_MQTT_SERVER;
const int   mqtt_port   = SECRET_MQTT_PORT;
const char* client_id   = SECRET_CLIENT_ID;


// NTP config
const char* ntp_server = "pool.ntp.org";
const char* tz_info    = "EST5EDT,M3.2.0,M11.1.0";  // Montreal / Eastern Time

WiFiClient espClient;
PubSubClient mqtt(espClient);
Adafruit_BME280 bme;

void connectWiFi() {
  Serial.print("Connecting to Wi-Fi");
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println();
  Serial.print("IP: "); Serial.println(WiFi.localIP());
}

void syncTime() {
  Serial.print("Syncing time via NTP");
  configTzTime(tz_info, ntp_server);

  struct tm timeinfo;
  while (!getLocalTime(&timeinfo, 1000)) {
    Serial.print(".");
  }
  Serial.println();
  Serial.print("Time synced: ");
  Serial.println(asctime(&timeinfo));
}

void connectMQTT() {
  while (!mqtt.connected()) {
    Serial.print("Connecting to MQTT...");
    if (mqtt.connect(client_id)) {
      Serial.println(" connected!");
    } else {
      Serial.print(" failed, rc=");
      Serial.print(mqtt.state());
      Serial.println(" — retrying in 5s");
      delay(5000);
    }
  }
}

// Fills buffer with ISO 8601 UTC timestamp: "2026-06-05T14:23:45Z"
void getTimestamp(char* buf, size_t len) {
  time_t now;
  time(&now);
  struct tm timeinfo;
  gmtime_r(&now, &timeinfo);   // UTC
  strftime(buf, len, "%Y-%m-%dT%H:%M:%SZ", &timeinfo);
}

void setup() {
  Serial.begin(115200);
  delay(1000);

  if (!bme.begin(0x76) && !bme.begin(0x77)) {
    Serial.println("No BME280 found!");
    while (1) delay(1000);
  }

  connectWiFi();
  syncTime();
  mqtt.setServer(mqtt_server, mqtt_port);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectWiFi();
  if (!mqtt.connected()) connectMQTT();
  mqtt.loop();

  float temp = bme.readTemperature();
  float hum  = bme.readHumidity();
  float pres = bme.readPressure() / 100.0F;

  char ts[32];
  getTimestamp(ts, sizeof(ts));

  char payload[192];
  snprintf(payload, sizeof(payload),
           "{\"ts\":\"%s\",\"temp\":%.2f,\"hum\":%.2f,\"pres\":%.2f,\"rssi\":%d}",
           ts, temp, hum, pres, WiFi.RSSI());

  mqtt.publish("greenhouse/node01/sensors", payload);
  Serial.print("Published: "); Serial.println(payload);

  delay(10000);
}