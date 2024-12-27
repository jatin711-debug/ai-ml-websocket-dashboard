import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from kafka import KafkaConsumer
import json
import pickle
from datetime import datetime
import logging
import asyncio
import websockets
import os

class PredictiveMaintenanceSystem:
    def __init__(self, websocket_port=3001):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = [
            'temperature', 'vibration', 'pressure', 'rotation_speed',
            'power_consumption', 'noise_level', 'operating_hours'
        ]
        self.websocket_port = websocket_port
        self.connected_clients = set()
        self.setup_logging()

        # Load pre-trained model if it exists
        self.load_model()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('maintenance_system.log'),
                logging.StreamHandler()
            ]
        )

    def load_model(self):
        """Load pre-trained model if available"""
        if os.path.exists('maintenance_model.pkl'):
            with open('maintenance_model.pkl', 'rb') as f:
                self.model, self.scaler = pickle.load(f)
            logging.info("Model loaded successfully.")
        else:
            logging.warning("No pre-trained model found, training a new model.")
            # Optionally, train a new model here if needed
            # self.train_model('historical_sensor_data.csv')

    async def register_client(self, websocket):
        """Register a new WebSocket client"""
        self.connected_clients.add(websocket)
        logging.info(f"New client connected. Total clients: {len(self.connected_clients)}")

    async def unregister_client(self, websocket):
        """Unregister a WebSocket client"""
        self.connected_clients.remove(websocket)
        logging.info(f"Client disconnected. Total clients: {len(self.connected_clients)}")

    async def handle_websocket_connection(self, websocket, path=None):
        """Handle WebSocket connections"""
        await self.register_client(websocket)
        try:
            await websocket.wait_closed()
        finally:
            await self.unregister_client(websocket)

    async def broadcast_prediction(self, prediction_result, sensor_data):
        """Broadcast prediction results to all connected clients"""
        if self.connected_clients:
            # Combine prediction results with sensor data
            message = {
                **prediction_result,
                'sensor_data': sensor_data
            }

            # Broadcast to all clients
            websockets_tasks = []
            for websocket in self.connected_clients.copy():
                try:
                    websockets_tasks.append(
                        asyncio.create_task(websocket.send(json.dumps(message)))
                    )
                except websockets.exceptions.ConnectionClosed:
                    await self.unregister_client(websocket)

            if websockets_tasks:
                await asyncio.gather(*websockets_tasks)

    def preprocess_data(self, data):
        """Preprocess raw sensor data for model training or prediction"""
        # Handle missing values
        data = data.fillna(method='ffill')

        # Create additional features
        data['temp_change'] = data['temperature'].diff()
        data['vibration_change'] = data['vibration'].diff()

        # Add time-based features
        data['hour'] = pd.to_datetime(data['timestamp']).dt.hour
        data['day_of_week'] = pd.to_datetime(data['timestamp']).dt.dayofweek

        return data

    def train_model(self, training_data_path):
        """Train the predictive maintenance model"""
        try:
            # Load and preprocess training data
            data = pd.read_csv(training_data_path)
            processed_data = self.preprocess_data(data)

            # Prepare features and target
            X = processed_data[self.feature_columns]
            y = processed_data['failure_status']

            # Split dataset
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )

            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)

            # Train model
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42
            )
            self.model.fit(X_train_scaled, y_train)

            # Evaluate model
            y_pred = self.model.predict(X_test_scaled)
            logging.info("Model Performance:\n" + classification_report(y_test, y_pred))

            # Save model
            with open('maintenance_model.pkl', 'wb') as f:
                pickle.dump((self.model, self.scaler), f)

            logging.info("Model training completed successfully")

        except Exception as e:
            logging.error(f"Error during model training: {str(e)}")
            raise

    def predict_maintenance(self, sensor_data):
        """Make predictions on new sensor data"""
        try:
            # Preprocess incoming data
            processed_data = pd.DataFrame([sensor_data])
            scaled_data = self.scaler.transform(processed_data[self.feature_columns])

            # Make prediction
            prediction = self.model.predict(scaled_data)[0]
            probability = self.model.predict_proba(scaled_data)[0][1]

            return {
                'prediction': int(prediction),
                'failure_probability': float(probability),
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logging.error(f"Error during prediction: {str(e)}")
            raise

    async def process_kafka_messages(self):
        """Process Kafka messages in the event loop"""
        try:
            consumer = KafkaConsumer(
                'sensor_data',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            logging.info("Started consuming from Kafka topic: sensor_data")

            for message in consumer:
                sensor_data = message.value
                prediction_result = self.predict_maintenance(sensor_data)

                if prediction_result['failure_probability'] > 0.7:
                    logging.warning(f"High failure probability detected: {prediction_result}")

                # Broadcast prediction to WebSocket clients
                await self.broadcast_prediction(prediction_result, sensor_data)
                await asyncio.sleep(1)
                logging.info(f"Processed and broadcast sensor data: {prediction_result}")

        except Exception as e:
            logging.error(f"Error in Kafka consumer: {str(e)}")
            raise

    async def start_websocket_server(self):
        async with websockets.serve(self.handle_websocket_connection, "0.0.0.0", self.websocket_port):
            logging.info(f"WebSocket server started on port {self.websocket_port}")
            await asyncio.Future()  # Run forever

    async def start_kafka_consumer(self):
        await self.process_kafka_messages()
        logging.info("Kafka consumer started on topic: sensor_data")

async def main():
    maintenance_system = PredictiveMaintenanceSystem()

    # Train the model (if not already trained)
    maintenance_system.train_model('historical_sensor_data.csv')

    # Start WebSocket and Kafka consumer tasks concurrently
    websocket_server_task = asyncio.create_task(maintenance_system.start_websocket_server())
    kafka_consumer_task = asyncio.create_task(maintenance_system.start_kafka_consumer())

    # Run all tasks
    await asyncio.gather(
        websocket_server_task,
        kafka_consumer_task
    )

if __name__ == "__main__":
    asyncio.run(main())
