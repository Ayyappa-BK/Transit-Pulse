import os
import json
import logging
from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, RichSinkFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, ValueState

import redis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables (ensure these match docker-compose)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/transitpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# High Traffic Bounding Box (Lat/Lon) - Downtown Chicago approx
BBOX_LAT_MIN = 41.875
BBOX_LAT_MAX = 41.890
BBOX_LON_MIN = -87.635
BBOX_LON_MAX = -87.615

class VelocityCalculator(KeyedProcessFunction):
    """
    Stateful function to calculate velocity based on previous location and timestamp.
    Keyed by 'vid' (Vehicle ID).
    """
    def __init__(self):
        self.last_state = None

    def open(self, runtime_context: RuntimeContext):
        # We store the previous state as a tuple: (latitude, longitude, timestamp_epoch)
        descriptor = ValueStateDescriptor("last_bus_state", Types.PICKLED_BYTE_ARRAY())
        self.last_state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        # Value is expected to be a JSON string or dict. 
        # For simplicity, assuming the input stream is deserialized to dict or we parse it here.
        # But Flink Kafka consumer usually gives string if using SimpleStringSchema.
        
        try:
            if isinstance(value, str):
                data = json.loads(value)
            else:
                data = value
            
            vid = data.get('vid')
            try:
                # lat/lon usually strings in CTA API
                lat = float(data.get('lat', 0.0))
                lon = float(data.get('lon', 0.0))
                # tmstmp is "YYYYMMDD HH:MM", need to parse to epoch
                import datetime
                tmstmp_str = data.get('tmstmp')
                dt = datetime.datetime.strptime(tmstmp_str, "%Y%m%d %H:%M")
                curr_ts = dt.timestamp()
            except Exception as e:
                # logger.error(f"Parse error: {e}")
                return # Skip bad records

            velocity = 0.0
            prev_state_bytes = self.last_state.value()
            
            if prev_state_bytes:
                import pickle
                prev_lat, prev_lon, prev_ts = pickle.loads(prev_state_bytes)
                
                # Calculate distance (Haversine approx or simple Euclidean for short distances)
                # Using simple Euclidean degree diff for speed in verification, but real should be Haversine
                # 1 deg lat ~ 111km, 1 deg lon ~ 85km at this lat
                
                d_lat = (lat - prev_lat) * 111000 # meters
                d_lon = (lon - prev_lon) * 85000  # meters
                dist = (d_lat**2 + d_lon**2)**0.5 # meters
                
                time_diff = curr_ts - prev_ts
                
                if time_diff > 0:
                    velocity = (dist / time_diff) * 3.6 # m/s to km/h
            
            # Update state
            import pickle
            new_state = (lat, lon, curr_ts)
            self.last_state.update(pickle.dumps(new_state))
            
            # Enrich data
            data['velocity_kmh'] = round(velocity, 2)
            
            # Spatial Logic check
            is_high_risk = False
            if (BBOX_LAT_MIN <= lat <= BBOX_LAT_MAX) and (BBOX_LON_MIN <= lon <= BBOX_LON_MAX):
                is_high_risk = True
            
            data['high_delay_risk'] = is_high_risk
            
            # Emit enriched data
            yield data
            
        except Exception as e:
            logger.error(f"Error in process_element: {e}")

class RedisSink(RichSinkFunction):
    """Custom Sink to update Redis with latest arrival predictions."""
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.r = None

    def open(self, runtime_context: RuntimeContext):
        self.r = redis.Redis(host=self.host, port=self.port, decode_responses=True)

    def invoke(self, value, context):
        try:
            # Value is the enriched dict
            vid = value.get('vid')
            route = value.get('rt')
            dest = value.get('des')
            pred_time = value.get('prdctdn', 'N/A') # predicted time in min or timestamp
            
            key = f"bus:{vid}"
            field_data = {
                "route": route,
                "dest": dest,
                "velocity": value.get('velocity_kmh'),
                "risk": str(value.get('high_delay_risk')),
                "last_updated": value.get('tmstmp')
            }
            # Use HSET for bus info
            self.r.hset(key, mapping=field_data)
            self.r.expire(key, 300) # Expire after 5 mins
        except Exception as e:
            # logger.error(f"Redis Sink Error: {e}")
            pass

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Note: Requires Flink Kafka Connector JAR and Postgres JDBC JAR driver to be present in env
    # e.g., via env.add_jars("file:///path/to/flink-sql-connector-kafka.jar", ...)
    # For now, we assume the runtime has them or user adds them.
    
    # 1. Source: Kafka
    kafka_props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'flink-processor-group'
    }
    
    kafka_source = FlinkKafkaConsumer(
        topics='raw_bus_locations',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    stream = env.add_source(kafka_source)
    
    # 2. Process: Velocity & Spatial Risk
    # Parse string to dict is handled inside KeyedProcessFunction for this flow, 
    # but we need to keyBy first. So let's map to json first to extract key.
    
    processed_stream = stream \
        .map(lambda x: json.loads(x), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .key_by(lambda x: x['vid']) \
        .process(VelocityCalculator())

    # 3. Sink: Postgres (Store history)
    # We need to map dict back to a Row or Tuple compatible with JDBC sink, or use a custom JDBC sink.
    # Standard JDBC Sink requires Row or Tuple.
    # Also need to construct the SQL Insert.
    
    # Let's use a simple map to prepare the tuple for JDBC
    def to_jdbc_row(data):
        # id is auto-increment SERIAL, so we don't pass it.
        # Schema: vehicle_id, route_id, direction_id, timestamp, latitude, longitude, geom, speed, is_delayed, weather_condition, temperature
        
        # Construct Geometry WKT or rely on Postgres to make point?
        # PostGIS accepts "SRID=4326;POINT(-87.6 41.8)" string.
        
        return (
            data.get('vid'),
            data.get('rt'),
            data.get('rtdir'),
            # timestamp needs to be formatted for SQL or passed as object if mapping handles it. 
            # String is often safest "YYYY-MM-DD HH:MM:SS"
            data.get('tmstmp'), # format matches default usually
            float(data.get('lat')),
            float(data.get('lon')),
            f"SRID=4326;POINT({data.get('lon')} {data.get('lat')})",
            float(data.get('velocity_kmh', 0.0)),
            bool(data.get('high_delay_risk', False)),
            data.get('weather', {}).get('condition', 'Unknown'),
            float(data.get('weather', {}).get('temperature', 0.0))
        )
        
    jdbc_input_stream = processed_stream.map(
        lambda data: to_jdbc_row(data),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.FLOAT(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
            Types.BOOLEAN(), Types.STRING(), Types.FLOAT()
        ])
    )
    
    jdbc_sink = JdbcSink.sink(
        """
        INSERT INTO bus_history 
        (vehicle_id, route_id, direction_id, timestamp, latitude, longitude, geom, speed, is_delayed, weather_condition, temperature) 
        VALUES (?, ?, ?, TO_TIMESTAMP(?, 'YYYYMMDD HH:MI'), ?, ?, ST_GeomFromEWKT(?), ?, ?, ?, ?)
        """,
        Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.FLOAT(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
            Types.BOOLEAN(), Types.STRING(), Types.FLOAT()
        ]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(POSTGRES_URL)
            .with_driver_name('org.postgresql.Driver')
            .with_user_name(POSTGRES_USER)
            .with_password(POSTGRES_PASSWORD)
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(200)
            .with_max_retries(5)
            .build()
    )
    
    jdbc_input_stream.add_sink(jdbc_sink)

    # 4. Sink: Redis (Real-time dashboard cache)
    processed_stream.add_sink(RedisSink(REDIS_HOST, REDIS_PORT))

    env.execute("TransitPulse Stream Processor")

if __name__ == "__main__":
    main()
