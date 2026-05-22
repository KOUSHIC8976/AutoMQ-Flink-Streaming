import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

def main():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    topic = 'automq_demo_topic'
    
    print("Streaming continuous telemetry stream into AutoMQ")
    
    try:
        while True:
            payload = {
                "satellite_id": "SAT-AUTOMQ-01",
                "temperature_c": round(random.uniform(130.0, 160.0), 2),
                "ts_string": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
            
            producer.produce(topic, json.dumps(payload).encode('utf-8'))
            producer.flush()
            
            print(f"Sent: {payload['temperature_c']}°C at {payload['ts_string']}")
            time.sleep(1) 
            
    except KeyboardInterrupt:
        print("\n Stream stopped manually.")

if __name__ == '__main__':
    main()