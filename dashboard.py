from flask import Flask, render_template
from resource_allocation import dashboard_data  # Import shared dashboard data
import threading

app = Flask(__name__)

# HTML template for the dashboard
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IoT Resource Allocation Dashboard</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f4f4f4;
        }
        h1 {
            text-align: center;
            color: #333;
        }
        h2 {
            color: #555;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background-color: #fff;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        th, td {
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #4CAF50;
            color: white;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .summary {
            background-color: #fff;
            padding: 15px;
            margin: 20px 0;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        .summary p {
            margin: 5px 0;
        }
    </style>
</head>
<body>
    <h1>IoT Resource Allocation Dashboard</h1>

    <h2>Summary</h2>
    <div class="summary">
        <p><strong>Total CPU Usage:</strong> {{ summary.total_cpu|round(2) }}%</p>
        <p><strong>Total Devices:</strong> {{ summary.total_devices }}</p>
        <p><strong>Average CPU Usage:</strong> {{ summary.avg_cpu|round(2) }}%</p>
        <p><strong>Number of Anomalies:</strong> {{ summary.num_anomalies }}</p>
        <h3>Top 5 Vehicles by CPU Usage</h3>
        <ul>
        {% for vehicle in summary.top_5 %}
            <li>Vehicle {{ vehicle.veh_id }}: {{ vehicle.cpu|round(2) }}%</li>
        {% endfor %}
        </ul>
    </div>

    <h2>Recent Allocations</h2>
    <table>
        <tr>
            <th>Vehicle ID</th>
            <th>Speed (m/s)</th>
            <th>Priority</th>
            <th>CPU (%)</th>
            <th>Memory (GB)</th>
            <th>Timestamp</th>
        </tr>
        {% for entry in allocations %}
        <tr>
            <td>{{ entry.veh_id }}</td>
            <td>{{ entry.spd|round(2) }}</td>
            <td>{{ entry.priority }}</td>
            <td>{{ entry.cpu|round(2) }}</td>
            <td>{{ entry.memory|round(2) }}</td>
            <td>{{ entry.timestamp }}</td>
        </tr>
        {% endfor %}
    </table>

    <h2>Recent Anomalies</h2>
    <table>
        <tr>
            <th>Vehicle ID</th>
            <th>Anomaly Message</th>
            <th>Timestamp</th>
        </tr>
        {% for anomaly in anomalies %}
        <tr>
            <td>{{ anomaly.veh_id }}</td>
            <td>{{ anomaly.message }}</td>
            <td>{{ anomaly.timestamp }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
"""

@app.route('/')
def dashboard():
    return render_template('dashboard.html',
                          allocations=dashboard_data["allocations"],
                          anomalies=dashboard_data["anomalies"],
                          summary=dashboard_data["summary"])

def run_flask():
    app.run(host='0.0.0.0', port=5000, debug=False)

if __name__ == "__main__":
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.start()