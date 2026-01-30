import os
import json
import time
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

CTA_API_KEY = os.getenv("CTA_API_KEY", "YOUR_CTA_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "YOUR_WEATHER_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CTA_BUS_API_URL = "http://www.ctabustracker.com/bustime/api/v2/getvehicles"
OPENWEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"
TOPIC_NAME = "raw_bus_locations"

# Weather Cache to avoid hitting API limit
weather_cache = {
    "data": None,
    "last_fetched": 0
}

def get_weather(lat, lon):
    """Fetches current weather for a location (Chicago default) with simple caching."""
    global weather_cache
    now = time.time()
    
    # Refresh weather every 10 minutes
    if weather_cache["data"] and (now - weather_cache["last_fetched"] < 600):
        return weather_cache["data"]

    try:
        # Hardcoding Chicago coords for simplicity in this broad call, 
        # or use the bus's location if granular weather is needed.
        # For efficiency, we'll fetch once for the general area.
        params = {
            "lat": 41.8781,
            "lon": -87.6298,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric"
        }
        resp = requests.get(OPENWEATHER_API_URL, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            weather_info = {
                "condition": data["weather"][0]["main"],
                "temperature": data["main"]["temp"],
                "wind_speed": data["wind"]["speed"]
            }
            weather_cache["data"] = weather_info
            weather_cache["last_fetched"] = now
            return weather_info
        else:
            logger.error(f"Weather API error: {resp.status_code} - {resp.text}")
    except Exception as e:
        logger.error(f"Failed to fetch weather: {e}")
    
    return weather_cache["data"] if weather_cache["data"] else {"condition": "Unknown", "temperature": 0.0}

def fetch_bus_data():
    """Polls CTA API for all vehicles."""
    try:
        # fetching specific routes or all? API usually requires rt (route) or vid (vehicle id).
        # We will query a set of popular routes for demo purposes.
        routes = "3,4,9,20,66,146,147,151" 
        params = {
            "key": CTA_API_KEY,
            "rt": routes,
            "format": "json"
        }
        resp = requests.get(CTA_BUS_API_URL, params=params, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            if "bustime-response" in data and "vehicle" in data["bustime-response"]:
                return data["bustime-response"]["vehicle"]
            else:
                logger.warning("No vehicles found in response.")
                return []
        else:
            logger.error(f"CTA API error: {resp.status_code} - {resp.text}")
            return []
    except Exception as e:
        logger.error(f"Failed to fetch bus data: {e}")
        return []

def delivery_report(err, msg):
    """Kafka delivery callback."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    # else:
    #     logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_topic_if_not_exists():
    """Creates the Kafka topic if it doesn't exist."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    topic_list = []
    topic_list.append(NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1))
    
    fs = admin_client.create_topics(topic_list)
    
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"Topic {topic} created")
        except Exception as e:
            # Continue if topic already exists
            if "Topic exists" not in str(e):
                logger.error(f"Failed to create topic {topic}: {e}")

def main():
    # Producer configuration
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'cta-producer-1'
    }
    
    producer = Producer(conf)
    create_topic_if_not_exists()
    
    logger.info("Starting CTA Bus Producer...")
    
    try:
        while True:
            # 1. Fetch Bus Data
            buses = fetch_bus_data()
            
            # 2. Fetch Weather Data (General for Chicago)
            weather = get_weather(41.8781, -87.6298)
            
            # 3. Produce to Kafka
            for bus in buses:
                # Enrich with weather
                bus['weather'] = weather
                bus['timestamp_produced'] = datetime.utcnow().isoformat()
                
                # Use vehicle ID as key
                key = bus.get('vid', str(time.time()))
                
                producer.produce(
                    TOPIC_NAME,
                    key=key,
                    value=json.dumps(bus),
                    on_delivery=delivery_report
                )
            
            producer.flush()
            
            count = len(buses)
            logger.info(f"Produced {count} bus records. Heartbeat: {datetime.now().strftime('%H:%M:%S')}")
            
            # CTA feed updates roughly every minute, but we can poll every 15-30s
            time.sleep(15)
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
