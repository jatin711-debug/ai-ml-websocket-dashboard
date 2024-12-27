import pandas as pd
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
        
        # Initialize base parameters for the machine (normal values)
        self.base_params = {
            'temperature': 60.0,  # Normal temperature
            'vibration': 1.0,     # Normal vibration
            'pressure': 100.0,    # Normal pressure
            'rotation_speed': 1100.0,  # Normal rotation speed
            'power_consumption': 80.0,  # Normal power consumption
            'noise_level': 70.0,     # Normal noise level
            'operating_hours': 0.0
        }
        
        # Deterioration rates to simulate wear and tear for error-prone readings
        self.deterioration_rates = {
            'temperature': 0.2,
            'vibration': 0.05,
            'pressure': -0.1,
            'rotation_speed': -1.0,
            'power_consumption': 0.5,
            'noise_level': 0.2
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
        
        # Randomly decide whether to generate a normal or error-prone reading (50/50 chance)
        is_error = np.random.rand() > 0.5  # 50% chance for error
        
        # If error-prone, apply higher values and rapid deterioration
        if is_error:
            for param, base_value in self.base_params.items():
                if param == 'operating_hours':
                    current_reading[param] = base_value
                    continue
                
                # Apply exaggerated deterioration for error state
                deterioration = self.deterioration_rates.get(param, 0) * 2 * (self.base_params['operating_hours'] / 24)  # Double deterioration
                
                # Add random noise with higher noise factor for errors
                noise_factor = {
                    'temperature': 5.0,  # Exaggerated noise for temperature
                    'vibration': 1.0,    # Exaggerated noise for vibration
                    'pressure': 2.0,     # Exaggerated noise for pressure
                    'rotation_speed': 50.0,  # Exaggerated noise for rotation speed
                    'power_consumption': 3.0,  # Exaggerated noise for power consumption
                    'noise_level': 3.0   # Exaggerated noise for noise level
                }.get(param, 0.1)
                
                noise = np.random.normal(0, noise_factor)
                
                # Calculate final error-prone value
                value = base_value + deterioration + noise
                current_reading[param] = round(value, 2)
        else:
            # For normal readings, generate values within expected ranges
            for param, base_value in self.base_params.items():
                if param == 'operating_hours':
                    current_reading[param] = base_value
                    continue
                
                # Apply normal deterioration
                deterioration = self.deterioration_rates.get(param, 0) * (self.base_params['operating_hours'] / 24)
                
                # Add random noise with standard noise factor
                noise_factor = {
                    'temperature': 2.0,
                    'vibration': 0.2,
                    'pressure': 1.0,
                    'rotation_speed': 20.0,
                    'power_consumption': 1.0,
                    'noise_level': 1.0
                }.get(param, 0.1)
                
                noise = np.random.normal(0, noise_factor)
                
                # Calculate final normal value
                value = base_value + deterioration + noise
                current_reading[param] = round(value, 2)

        return current_reading

    def simulate_maintenance(self):
        """Reset parameters after maintenance"""
        logging.info("Simulating maintenance - resetting parameters")
        self.base_params = {
            'temperature': np.random.uniform(60, 65),  # Normal range after maintenance
            'vibration': np.random.uniform(1.0, 1.2),  # Normal range after maintenance
            'pressure': np.random.uniform(98, 102),    # Normal range after maintenance
            'rotation_speed': np.random.uniform(1100, 1150),  # Normal range after maintenance
            'power_consumption': np.random.uniform(78, 82),  # Normal range after maintenance
            'noise_level': np.random.uniform(68, 72),  # Normal range after maintenance
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
    producer.start_producing(interval_seconds=3)

if __name__ == "__main__":
    main()
