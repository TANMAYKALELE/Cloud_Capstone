import time
import hashlib
import random
import os

blockchain = []

def hash_block(block):
    block_string = f"{block['index']}{block['data']}{block['timestamp']}{block['prev_hash']}"
    return hashlib.sha256(block_string.encode()).hexdigest()

def simulate_iot_demand(device_id, previous_demand=None, priority=1):
    # Simulate dynamic demand with priority-based weighted optimization
    if previous_demand is None:
        demand_level = random.randint(1, 100)
    else:
        demand_change = random.randint(-20, 20)
        demand_level = max(1, min(100, previous_demand + demand_change))
    # Weighted adjustment based on priority (1=low, 2=medium, 3=high)
    weight = {1: 1.0, 2: 1.2, 3: 1.5}[priority]
    adjusted_demand = min(100, demand_level * weight)
    cpu = min(90, int(adjusted_demand * 1.1))  # Cap at 90%
    memory = random.choice([2, 4, 6]) + (priority - 1)  # More memory for higher priority
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    allocation_data = f"{device_id} allocated CPU:{cpu}%, Memory:{memory}GB at {timestamp}"
    return allocation_data, timestamp, demand_level

def detect_anomaly(cpu, device_id):
    # Detect anomaly with severity level
    anomaly_threshold = 85
    previous_blocks = [b for b in blockchain if device_id in b['data']]
    if previous_blocks:
        prev_cpu = int(previous_blocks[-1]['data'].split("CPU:")[1].split("%")[0])
        spike = abs(cpu - prev_cpu) > 30
    else:
        spike = False
    if cpu > anomaly_threshold or spike:
        severity = "Low" if 85 <= cpu <= 90 else "Medium" if 90 < cpu <= 95 else "High"
        return True, severity
    return False, None

def log_allocation(device_id, cpu=None, memory=None, priority=1, previous_demand=None):
    if cpu is None or memory is None:
        allocation_data, timestamp, demand_level = simulate_iot_demand(device_id, previous_demand, priority)
    else:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        allocation_data = f"{device_id} allocated CPU:{cpu}%, Memory:{memory}GB at {timestamp}"
        demand_level = previous_demand
    
    cpu_value = int(allocation_data.split("CPU:")[1].split("%")[0])
    anomaly, severity = detect_anomaly(cpu_value, device_id)
    if anomaly:
        print(f"WARNING: Anomaly detected for {device_id} with CPU:{cpu_value}%! Severity: {severity}")

    index = len(blockchain)
    prev_hash = blockchain[-1]['hash'] if blockchain else "0" * 64
    block = {'index': index, 'data': allocation_data, 'timestamp': timestamp, 'prev_hash': prev_hash}
    block['hash'] = hash_block(block)
    blockchain.append(block)
    # Write to simulated Hadoop output file
    with open("output.txt", "a") as f:
        f.write(f"{allocation_data}\n")
    print(f"Block {index}: {allocation_data}, Hash: {block['hash'][:6]}...")
    return demand_level

def log_hadoop_summary(total_cpu, device_count):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    summary_data = f"Summary: Total CPU {total_cpu}% across {device_count} devices at {timestamp}"
    index = len(blockchain)
    prev_hash = blockchain[-1]['hash'] if blockchain else "0" * 64
    block = {'index': index, 'data': summary_data, 'timestamp': timestamp, 'prev_hash': prev_hash}
    block['hash'] = hash_block(block)
    blockchain.append(block)
    with open("output.txt", "a") as f:
        f.write(f"{summary_data}\n")
    print(f"Block {index}: {summary_data}, Hash: {block['hash'][:6]}...")

def verify_blockchain():
    print("\nVerifying Blockchain Integrity...")
    tampered = False
    for i, block in enumerate(blockchain):
        computed_hash = hash_block(block)
        if block['hash'] != computed_hash:
            print(f"Tampering detected at Block {i}!")
            tampered = True
        if i > 0 and block['prev_hash'] != blockchain[i-1]['hash']:
            print(f"Chain broken at Block {i}!")
            tampered = True
    if not tampered:
        print("Blockchain is intact and secure!")
    return not tampered

# Initial simulated allocations with demand updates and priorities
print("Starting with simulated IoT allocations...")
devices = {"Sensor1": 1, "Camera1": 2, "SecurityCam": 3}  # Priority: 1=low, 2=medium, 3=high
demand_levels = {device: None for device in devices}

# Simulate a few rounds of demand updates
for _ in range(2):
    total_cpu = 0
    for device, priority in devices.items():
        demand_levels[device] = log_allocation(device, priority=priority, previous_demand=demand_levels[device])
        total_cpu += int(blockchain[-1]['data'].split("CPU:")[1].split("%")[0])
    log_hadoop_summary(total_cpu, len(devices))
    time.sleep(1)

# Real-time user input feature
while True:
    user_input = input("\nEnter device ID (or 'quit' to stop): ")
    if user_input.lower() == 'quit':
        break
    priority = int(input("Enter priority (1-3, 1=low, 3=high): "))
    if priority not in [1, 2, 3]:
        print("Priority must be 1, 2, or 3!")
        continue
    cpu = input("Enter CPU percentage (e.g., 50): ")
    memory = input("Enter memory in GB (e.g., 2): ")
    try:
        cpu = int(cpu)
        memory = int(memory)
        if 0 <= cpu <= 100 and memory > 0:
            log_allocation(user_input, cpu, memory, priority)
            total_cpu = sum(int(b['data'].split("CPU:")[1].split("%")[0]) for b in blockchain if "allocated" in b['data'])
            log_hadoop_summary(total_cpu, len(devices))
        else:
            print("Invalid input! CPU must be 0-100%, Memory must be positive.")
    except ValueError:
        print("Please enter valid numbers!")

# Verify the blockchain
verify_blockchain()
print("\nBlockchain Log Completed!")