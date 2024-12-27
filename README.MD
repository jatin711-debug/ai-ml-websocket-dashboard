# Predictive Maintenance System

This project implements a predictive maintenance system that uses sensor data to predict potential machine failures. It integrates with Kafka for real-time data streaming and utilizes a Random Forest Classifier for failure prediction. The system uses WebSocket to broadcast predictions in real-time to connected clients.

## Features

- **Real-time Prediction**: Predicts failure probability of machines based on sensor data.
- **Kafka Integration**: Consumes real-time sensor data from a Kafka topic.
- **WebSocket Server**: Broadcasts prediction results to connected clients.
- **Model Training**: Uses historical sensor data to train a machine learning model for failure prediction.

## Prerequisites

Before running this system, ensure you have the following installed:

- **Python 3.8+**
- **Kafka Broker**: Running Kafka instance on `localhost:9092`.

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/predictive-maintenance-system.git
cd predictive-maintenance-system
