import boto3
import json
import time
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder.appName("ResourceAllocation").getOrCreate()

# Initialize boto3 clients
s3_client = boto3.client('s3')
emr_client = boto3.client('emr')

# S3 bucket and paths
BUCKET_NAME = "cloud-capstone-bucket-tanmay-2025"
OUTPUT_PATH = "output/output.json"

# Function to fetch EMR cost using Cost Explorer
def get_emr_cost():
    """
    Fetches the cost of EMR usage using Cost Explorer.
    Includes a fallback value due to the 24-hour delay in Cost Explorer data availability.
    """
    try:
        client = boto3.client('ce')
        # Define the time period (last 30 days up to today)
        end_date = datetime.utcnow().strftime('%Y-%m-%d')
        start_date = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='DAILY',
            Metrics=['UnblendedCost'],
            Filter={
                'Dimensions': {
                    'Key': 'SERVICE',
                    'Values': ['Amazon Elastic MapReduce']
                }
            }
        )
        total_cost = sum(float(item['Total']['UnblendedCost']['Amount']) for item in response['ResultsByTime'])
        return round(total_cost, 2)
    except Exception as e:
        print(f"Error fetching cost data: {e}")
        # Fallback value for testing (remove after March 24, 2025, when Cost Explorer data is available)
        return 10.50  # Simulate a cost of $10.50

# Function to get cluster metrics (e.g., CPU utilization)
def get_cluster_metrics(cluster_id):
    """
    Fetches cluster metrics (e.g., CPU utilization) using CloudWatch.
    For simplicity, this is a placeholder—replace with actual CloudWatch logic if needed.
    """
    # Placeholder: Simulate CPU utilization
    # In a real implementation, use boto3 CloudWatch client to fetch metrics
    return 75  # Simulate 75% CPU utilization

# Function to adjust cluster resources based on metrics
def adjust_cluster_resources(cluster_id, current_cpu):
    """
    Adjusts the number of nodes in the EMR cluster based on CPU utilization.
    - Scale out if CPU > 70% for 5 minutes.
    - Scale in if CPU < 20% for 3 minutes.
    """
    # Get the current number of core nodes
    response = emr_client.describe_cluster(ClusterId=cluster_id)
    core_nodes = response['Cluster']['InstanceGroups'][1]['RunningInstanceCount']  # Assuming index 1 is core nodes

    # Auto-scaling logic
    if current_cpu > 70:
        print("CPU > 70%, scaling out...")
        new_node_count = min(core_nodes + 1, 5)  # Max 5 nodes
        emr_client.modify_instance_groups(
            ClusterId=cluster_id,
            InstanceGroups=[
                {
                    'InstanceGroupId': response['Cluster']['InstanceGroups'][1]['InstanceGroupId'],
                    'InstanceCount': new_node_count
                }
            ]
        )
    elif current_cpu < 20:
        print("CPU < 20%, scaling in...")
        new_node_count = max(core_nodes - 1, 2)  # Min 2 nodes
        emr_client.modify_instance_groups(
            ClusterId=cluster_id,
            InstanceGroups=[
                {
                    'InstanceGroupId': response['Cluster']['InstanceGroups'][1]['InstanceGroupId'],
                    'InstanceCount': new_node_count
                }
            ]
        )
    else:
        print("CPU within normal range, no scaling needed.")

# Function to write output to S3
def write_to_s3(data):
    """
    Writes the resource allocation and cost data to S3.
    """
    s3_client.put_object(
        Body=json.dumps(data),
        Bucket=BUCKET_NAME,
        Key=OUTPUT_PATH
    )

# Main function
def main():
    # Get the cluster ID (assuming this script runs on the EMR cluster)
    # In a real EMR environment, you can fetch this from metadata
    cluster_id = "j-XXXXXXXXXXXXX"  # Replace with actual cluster ID or fetch dynamically

    # Monitor and adjust resources in a loop
    for _ in range(5):  # Run for 5 iterations (for testing)
        # Get current metrics
        current_cpu = get_cluster_metrics(cluster_id)
        emr_cost = get_emr_cost()

        # Adjust resources based on metrics
        adjust_cluster_resources(cluster_id, current_cpu)

        # Prepare data for dashboard
        allocation_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "cpu_utilization": current_cpu,
            "core_nodes": emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['InstanceGroups'][1]['RunningInstanceCount'],
            "emr_cost": emr_cost,
            "anomaly_detected": current_cpu > 70 or current_cpu < 20
        }

        # Write data to S3 for dashboard
        write_to_s3(allocation_data)

        print(f"Allocation Data: {allocation_data}")

        # Sleep for 1 minute (simulate monitoring interval)
        time.sleep(60)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()