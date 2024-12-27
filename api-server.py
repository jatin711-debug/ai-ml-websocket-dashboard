from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import random

app = Flask(__name__)

# Simulated historical data generation
def generate_historical_data(time_range):
    # Get current time
    now = datetime.now()

    # Define the time period for historical data based on time_range
    if time_range == '24h':
        start_time = now - timedelta(hours=24)
    elif time_range == '1w':
        start_time = now - timedelta(weeks=1)
    elif time_range == '1m':
        start_time = now - timedelta(weeks=4)
    else:
        start_time = now - timedelta(hours=24)  # Default to 24h

    # Generate random historical data within the specified time range
    data = []
    for i in range(100):  # Assume 100 data points
        timestamp = start_time + timedelta(minutes=i * (24 * 60 // 100))  # Uniformly distributed
        data_point = {
            'timestamp': timestamp.isoformat(),
            'failure_probability': round(random.uniform(0, 1), 2),
            'temperature': round(random.uniform(60, 80), 2),
            'vibration': round(random.uniform(0.5, 1.5), 2),
            'pressure': round(random.uniform(95, 105), 2),
            'rotation_speed': round(random.uniform(1000, 1200), 2),
            'power_consumption': round(random.uniform(70, 90), 2),
            'noise_level': round(random.uniform(65, 75), 2)
        }
        data.append(data_point)

    return data

# Function to calculate health status based on historical data
def calculate_health_status(historical_data):
    total_failure_probability = 0
    high_risk_count = 0
    total_temperature = 0
    high_temperature_count = 0

    for data_point in historical_data:
        total_failure_probability += data_point['failure_probability']
        total_temperature += data_point['temperature']
        
        if data_point['failure_probability'] > 0.7:
            high_risk_count += 1
        
        if data_point['temperature'] > 75:
            high_temperature_count += 1

    average_failure_probability = total_failure_probability / len(historical_data)
    average_temperature = total_temperature / len(historical_data)

    health_status = {
        'average_failure_probability': round(average_failure_probability, 2),
        'average_temperature': round(average_temperature, 2),
        'high_risk_count': high_risk_count,
        'high_temperature_count': high_temperature_count
    }

    if average_failure_probability > 0.7 or high_temperature_count > 10:  # Threshold for risk
        health_status['status'] = 'At Risk'
    else:
        health_status['status'] = 'Healthy'

    return health_status

@app.route('/api/historical-data', methods=['GET'])
def historical_data():
    time_range = request.args.get('timeRange', default='24h', type=str)
    
    if time_range not in ['24h', '1w', '1m']:
        return jsonify({'error': 'Invalid timeRange parameter. Please use "24h", "1w", or "1m".'}), 400
    
    historical_data = generate_historical_data(time_range)
    
    return jsonify(historical_data)

@app.route('/api/get-health-status', methods=['GET'])
def get_health_status():
    time_range = request.args.get('timeRange', default='24h', type=str)
    
    if time_range not in ['24h', '1w', '1m']:
        return jsonify({'error': 'Invalid timeRange parameter. Please use "24h", "1w", or "1m".'}), 400
    
    historical_data = generate_historical_data(time_range)
    health_status = calculate_health_status(historical_data)
    
    return jsonify(health_status)

if __name__ == '__main__':
    app.run(debug=True)
