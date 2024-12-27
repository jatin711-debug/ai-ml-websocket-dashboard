import React, { useState, useEffect, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { AlertTriangle, CheckCircle, WifiOff } from 'lucide-react';
import * as XLSX from 'xlsx';

const PredictionDashboard = () => {
  const [predictionData, setPredictionData] = useState([]);
  const [historicalData, setHistoricalData] = useState([]);
  const [lastPrediction, setLastPrediction] = useState(null);
  const [isConnected, setIsConnected] = useState(false);
  const [timeRange, setTimeRange] = useState('24h');
  const [temperatureThreshold, setTemperatureThreshold] = useState(75);
  const [maintenanceLog, setMaintenanceLog] = useState([]);
  const [metrics, setMetrics] = useState({ accuracy: 0.9, precision: 0.8, recall: 0.85, f1Score: 0.82 });
  const [healthStatus, setHealthStatus] = useState(null); // For storing health status

  // WebSocket connection setup
  const connectWebSocket = useCallback(() => {
    const ws = new WebSocket('ws://127.0.0.1:3001');

    ws.onopen = () => {
      console.log('Connected to WebSocket');
      setIsConnected(true);
    };

    ws.onclose = () => {
      console.log('Disconnected from WebSocket');
      setIsConnected(false);
      setTimeout(connectWebSocket, 5000); // Reconnect after 5 seconds
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
      setIsConnected(false);
    };

    ws.onmessage = (event) => {
      const prediction = JSON.parse(event.data);
      setPredictionData((prevData) => [...prevData, prediction].slice(-50)); // Keep last 50 entries
      setLastPrediction(prediction);
    };

    return () => ws.close();
  }, []);

  useEffect(() => {
    connectWebSocket();
    return () => {
      setIsConnected(false);
    };
  }, [connectWebSocket]);

  const formatTimestamp = (timestamp) => {
    const date = new Date(timestamp);
    return `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`;
  };

  const getStatusColor = (probability) => {
    if (probability < 0.3) return 'text-green-500';
    if (probability < 0.7) return 'text-yellow-500';
    return 'text-red-500';
  };

  const getHealthStatus = (failureProbability) => {
    if (failureProbability < 0.3) return 'green';
    if (failureProbability < 0.7) return 'yellow';
    return 'red';
  };

  const handleThresholdChange = (e) => {
    setTemperatureThreshold(e.target.value);
  };

  const exportToCSV = () => {
    const ws = XLSX.utils.json_to_sheet(predictionData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Predictions');
    XLSX.writeFile(wb, 'prediction_data.xlsx');
  };

  const logMaintenance = () => {
    setMaintenanceLog((prevLog) => [
      ...prevLog,
      { time: new Date().toISOString(), action: 'Maintenance reset' },
    ]);
  };

  // Fetch historical data based on selected time range
  useEffect(() => {
    fetch(`/api/historical-data?timeRange=${timeRange}`)
      .then((res) => res.json())
      .then(setHistoricalData);
  }, [timeRange]);

  // Fetch health status data from API
  useEffect(() => {
    fetch(`/api/get-health-status?timeRange=${timeRange}`)
      .then((res) => res.json())
      .then(setHealthStatus);
  }, [timeRange]);

  return (
    <div className="min-h-screen bg-gray-100 p-6 flex flex-col gap-6">
      {/* Header */}
      <div className="flex justify-between items-center bg-white p-4 rounded-lg shadow-md">
        <h1 className="text-2xl font-bold text-gray-800">Predictive Maintenance Dashboard</h1>
        <div className="flex items-center gap-2">
          {isConnected ? (
            <CheckCircle className="text-green-500" size={24} />
          ) : (
            <WifiOff className="text-red-500" size={24} />
          )}
          <span className={isConnected ? 'text-green-500' : 'text-red-500'}>
            {isConnected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </div>

      {/* Last Prediction */}
      {lastPrediction && (
        <div className="bg-white p-4 rounded-lg shadow-md flex items-center gap-4">
          {lastPrediction.prediction === 0 ? (
            <CheckCircle className="text-green-500" size={32} />
          ) : (
            <AlertTriangle className="text-red-500" size={32} />
          )}
          <div>
            <h2 className="text-lg font-semibold">
              {(lastPrediction.failure_probability * 100).toFixed(2)}% Risk
            </h2>
            <p className={`text-sm ${getStatusColor(lastPrediction.failure_probability)}`}>
              {lastPrediction.prediction === 0
                ? 'System is operating normally.'
                : 'Potential failure detected.'}
            </p>
          </div>
        </div>
      )}

      {/* Health Status */}
      {healthStatus && (
        <div className="bg-white p-4 rounded-lg shadow-md">
          <h2 className="text-lg font-bold mb-4">System Health Status</h2>
          <div>
            <p className="text-sm font-medium">Average Failure Probability: {healthStatus.average_failure_probability * 100}%</p>
            <p className="text-sm font-medium">Average Temperature: {healthStatus.average_temperature}°C</p>
            <p className="text-sm font-medium">High Risk Events: {healthStatus.high_risk_count}</p>
            <p className="text-sm font-medium">High Temperature Events: {healthStatus.high_temperature_count}</p>
            <h3 className={`text-xl font-bold ${healthStatus.status === 'At Risk' ? 'text-red-500' : 'text-green-500'}`}>
              Status: {healthStatus.status}
            </h3>
          </div>
        </div>
      )}

      {/* Line Chart */}
      <div className="bg-white p-4 rounded-lg shadow-md">
        <h2 className="text-lg font-bold mb-4">Failure Probability Over Time</h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={predictionData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="timestamp"
              tickFormatter={formatTimestamp}
              interval="preserveEnd"
              tick={{ fontSize: 12 }}
            />
            <YAxis domain={[0, 1]} tickFormatter={(value) => `${(value * 100).toFixed(0)}%`} />
            <Tooltip
              labelFormatter={formatTimestamp}
              formatter={(value) => [`${(value * 100).toFixed(2)}%`, 'Failure Risk']}
            />
            <Line type="monotone" dataKey="failure_probability" stroke="#2563eb" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Sensor Data */}
      {lastPrediction?.sensor_data && (
        <div className="bg-white p-4 rounded-lg shadow-md">
          <h2 className="text-lg font-bold mb-4">Latest Sensor Data</h2>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            {Object.entries(lastPrediction.sensor_data).map(([key, value]) => (
              <div key={key} className="p-4 bg-gray-50 rounded-lg shadow-sm">
                <h3 className="text-sm font-medium text-gray-500">
                  {key.replace('_', ' ').toUpperCase()}
                </h3>
                <p className="text-lg font-semibold text-gray-800">
                  {typeof value === 'number' ? value.toFixed(2) : value}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Maintenance Log */}
      <div className="bg-white p-4 rounded-lg shadow-md">
        <h2 className="text-lg font-bold mb-4">Maintenance Log</h2>
        <ul>
          {maintenanceLog.map((log, idx) => (
            <li key={idx} className="text-sm text-gray-600">
              {log.time}: {log.action}
            </li>
          ))}
        </ul>
      </div>

      {/* Model Performance Metrics */}
      <div className="bg-white p-4 rounded-lg shadow-md">
        <h2 className="text-lg font-bold mb-4">Model Performance Metrics</h2>
        <ul>
          <li>Accuracy: {metrics.accuracy}</li>
          <li>Precision: {metrics.precision}</li>
          <li>Recall: {metrics.recall}</li>
          <li>F1-Score: {metrics.f1Score}</li>
        </ul>
      </div>

      {/* Control Panel for Thresholds and Time Range */}
      <div className="bg-white p-4 rounded-lg shadow-md flex gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700">Temperature Threshold</label>
          <input
            type="number"
            value={temperatureThreshold}
            onChange={handleThresholdChange}
            className="mt-1 block w-24 border border-gray-300 rounded-lg p-2"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700">Time Range</label>
          <select
            value={timeRange}
            onChange={(e) => setTimeRange(e.target.value)}
            className="mt-1 block w-40 border border-gray-300 rounded-lg p-2"
          >
            <option value="24h">Last 24 Hours</option>
            <option value="1w">Last 1 Week</option>
            <option value="1m">Last 1 Month</option>
          </select>
        </div>
      </div>

      {/* Export Button */}
      <div className="bg-white p-4 rounded-lg shadow-md flex justify-end">
        <button
          onClick={exportToCSV}
          className="bg-blue-500 text-white px-4 py-2 rounded-lg shadow-md hover:bg-blue-700"
        >
          Export Data
        </button>
      </div>
    </div>
  );
};

export default PredictionDashboard;
