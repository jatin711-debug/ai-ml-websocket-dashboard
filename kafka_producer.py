from kafka import KafkaProducer
import json
import time
import numpy as np
from datetime import datetime
import logging

class SensorDataProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='sensor_data'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.setup_logging()
        
        # Initialize base parameters for the machine
        self.base_params = {
            'temperature': 60.0,
            'vibration': 1.0,
            'pressure': 100.0,
            'rotation_speed': 1100.0,
            'power_consumption': 80.0,
            'noise_level': 70.0,
            'operating_hours': 0.0
        }
        
        # Deterioration rates per hour
        self.deterioration_rates = {
            'temperature': 0.1,
            'vibration': 0.01,
            'pressure': -0.05,
            'rotation_speed': -0.5,
            'power_consumption': 0.2,
            'noise_level': 0.1
        }

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sensor_producer.log'),
                logging.StreamHandler()
            ]
        )

    def generate_sensor_reading(self):
        """Generate a single sensor reading with realistic variations"""
        current_reading = {}
        
        # Update operating hours
        self.base_params['operating_hours'] += 1/60  # Assuming readings every minute
        
        # Generate readings for each sensor
        for param, base_value in self.base_params.items():
            if param == 'operating_hours':
                current_reading[param] = base_value
                continue
                
            # Add deterioration effect
            deterioration = self.deterioration_rates.get(param, 0) * (self.base_params['operating_hours'] / 24)
            
            # Add random noise
            noise_factor = {
                'temperature': 1.0,
                'vibration': 0.1,
                'pressure': 0.5,
                'rotation_speed': 10.0,
                'power_consumption': 1.0,
                'noise_level': 0.5
            }.get(param, 0.1)
            
            noise = np.random.normal(0, noise_factor)
            
            # Calculate final value
            value = base_value + deterioration + noise
            current_reading[param] = round(value, 2)

        return current_reading

    def simulate_maintenance(self):
        """Reset parameters after maintenance"""
        logging.info("Simulating maintenance - resetting parameters")
        self.base_params = {
            'temperature': np.random.uniform(58, 62),
            'vibration': np.random.uniform(0.9, 1.1),
            'pressure': np.random.uniform(98, 102),
            'rotation_speed': np.random.uniform(1080, 1120),
            'power_consumption': np.random.uniform(78, 82),
            'noise_level': np.random.uniform(68, 72),
            'operating_hours': 0.0
        }

    def start_producing(self, interval_seconds=60):
        """Start producing sensor data at specified interval"""
        logging.info(f"Starting sensor data production with {interval_seconds} second interval")
        
        try:
            while True:
                # Generate sensor reading
                reading = self.generate_sensor_reading()
                
                # Add timestamp
                reading['timestamp'] = datetime.now().isoformat()
                
                # Send to Kafka
                self.producer.send(self.topic, reading)
                logging.info(f"Sent sensor reading: {reading}")
                
                # Simulate maintenance if conditions are critical
                if (reading['temperature'] > 75 or 
                    reading['vibration'] > 1.5 or 
                    reading['operating_hours'] > 168):  # 1 week
                    self.simulate_maintenance()
                
                # Wait for next interval
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logging.info("Stopping sensor data production")
            self.producer.close()
        except Exception as e:
            logging.error(f"Error in data production: {str(e)}")
            self.producer.close()
            raise

def main():
    # Create and start the producer
    producer = SensorDataProducer()
    
    # Set interval to 10 seconds for testing (adjust as needed)
    producer.start_producing(interval_seconds=10)

if __name__ == "__main__":
    main()