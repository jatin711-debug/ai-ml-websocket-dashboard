import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sensor_data(num_machines=5, days=30, readings_per_hour=6):
    """
    Generate synthetic sensor data for predictive maintenance
    """
    # Calculate total number of readings
    total_readings = num_machines * days * 24 * readings_per_hour
    
    # Create timestamp range
    start_date = datetime(2024, 1, 1)
    timestamps = [start_date + timedelta(minutes=10*i) for i in range(total_readings)]
    
    # Initialize data lists
    data = []
    
    for machine_id in range(num_machines):
        # Base parameters for this machine
        base_temp = np.random.uniform(50, 70)
        base_vibration = np.random.uniform(0.5, 1.5)
        base_pressure = np.random.uniform(95, 105)
        base_rotation = np.random.uniform(1000, 1200)
        base_power = np.random.uniform(75, 85)
        base_noise = np.random.uniform(65, 75)
        
        # Operating hours counter
        operating_hours = 0
        
        # Deterioration factors
        temp_deterioration = np.random.uniform(0.01, 0.03)
        vibration_deterioration = np.random.uniform(0.005, 0.015)
        
        for i in range(days * 24 * readings_per_hour):
            # Add some random variation
            time_factor = i / (days * 24 * readings_per_hour)  # Normalized time factor
            
            # Simulate gradual deterioration
            temperature = base_temp + np.random.normal(0, 2) + (time_factor * temp_deterioration * 100)
            vibration = base_vibration + np.random.normal(0, 0.1) + (time_factor * vibration_deterioration * 10)
            pressure = base_pressure + np.random.normal(0, 1)
            rotation_speed = base_rotation + np.random.normal(0, 20)
            power_consumption = base_power + np.random.normal(0, 2)
            noise_level = base_noise + np.random.normal(0, 1)
            
            # Increment operating hours
            operating_hours += (1 / readings_per_hour)
            
            # Determine failure status (more likely with higher temperature and vibration)
            failure_prob = (temperature - base_temp) / 50 + (vibration - base_vibration) / 2
            failure_status = 1 if failure_prob > 0.7 or np.random.random() < 0.01 else 0
            
            # Add maintenance reset effect
            if failure_status == 1:
                base_temp = np.random.uniform(50, 70)
                base_vibration = np.random.uniform(0.5, 1.5)
                operating_hours = 0
            
            data.append({
                'timestamp': timestamps[i],
                'machine_id': f'MACHINE_{machine_id+1}',
                'temperature': temperature,
                'vibration': vibration,
                'pressure': pressure,
                'rotation_speed': rotation_speed,
                'power_consumption': power_consumption,
                'noise_level': noise_level,
                'operating_hours': operating_hours,
                'failure_status': failure_status
            })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Sort by timestamp
    df = df.sort_values('timestamp')
    
    # Add some random missing values
    for column in ['temperature', 'vibration', 'pressure', 'rotation_speed', 'power_consumption', 'noise_level']:
        mask = np.random.random(len(df)) < 0.01
        df.loc[mask, column] = np.nan
    
    return df

def main():
    # Generate data
    print("Generating sensor data...")
    df = generate_sensor_data(num_machines=5, days=30, readings_per_hour=6)
    
    # Save to CSV
    output_file = 'historical_sensor_data.csv'
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")
    
    # Print some statistics
    print("\nDataset Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Number of machines: {df['machine_id'].nunique()}")
    print(f"Number of failure events: {df['failure_status'].sum()}")
    print("\nSample of the generated data:")
    print(df.head())

if __name__ == "__main__":
    main()