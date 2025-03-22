from datetime import datetime

def read_iot_data(file_path):
    devices = []
    with open(file_path, 'r') as file:
        for line in file:
            device, cpu, mem, timestamp = line.strip().split(',')
            cpu_value = float(cpu.split(':')[1].replace('%', ''))
            mem_value = mem.split(':')[1]
            devices.append((device, cpu_value, mem_value, timestamp))
    return devices

def simulate_iot_demand(device, priority, cpu, mem):
    weights = {3: 1.5, 2: 1.2, 1: 1.0}
    weight = weights.get(priority, 1.0)
    adjusted_cpu = min(cpu * weight, 90 if priority == 3 else 75 if priority == 2 else 50)
    return adjusted_cpu, mem

def detect_anomaly(cpu, previous_cpu=None):
    if cpu > 85:
        return "High", "WARNING: Anomaly detected! Possible cyberattack."
    if previous_cpu and (cpu - previous_cpu) > 30:
        return "Medium", "WARNING: Significant CPU spike detected."
    return "Normal", "No anomaly detected."

def main():
    devices = read_iot_data("../input/iot_data.txt")
    output_lines = []
    previous_cpus = {}

    for device, cpu, mem, timestamp in devices:
        priority = 3 if "EngineSensor" in device else 2 if "Sensor" in device or "WeatherStation" in device else 1
        previous_cpu = previous_cpus.get(device)
        adjusted_cpu, adjusted_mem = simulate_iot_demand(device, priority, cpu, mem)
        severity, anomaly_message = detect_anomaly(adjusted_cpu, previous_cpu)
        previous_cpus[device] = adjusted_cpu

        log_entry = f"{device} allocated CPU:{adjusted_cpu}%, Memory:{mem} at {timestamp}"
        output_lines.append(log_entry)
        if severity != "Normal":
            output_lines.append(anomaly_message)

    total_cpu = sum(float(line.split("CPU:")[1].split("%")[0]) for line in output_lines if "allocated" in line)
    summary = f"Summary: Total CPU {total_cpu}% across {len(devices)} devices at {devices[-1][3]}"
    output_lines.append(summary)

    with open("../output.txt", "w") as f:
        f.write("\n".join(output_lines))

    print("Resource allocation completed. Check output.txt for details.")

    while True:
        device = input("Enter device name (or 'exit' to quit): ")
        if device.lower() == 'exit':
            break
        priority = int(input("Enter priority (1-3): "))
        cpu = float(input("Enter CPU usage (%): "))
        mem = float(input("Enter memory usage (GB): "))
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        previous_cpu = previous_cpus.get(device)
        adjusted_cpu, adjusted_mem = simulate_iot_demand(device, priority, cpu, f"{mem}GB")
        severity, anomaly_message = detect_anomaly(adjusted_cpu, previous_cpu)
        previous_cpus[device] = adjusted_cpu

        log_entry = f"{device} allocated CPU:{adjusted_cpu}%, Memory:{mem}GB at {timestamp}"
        print(log_entry)
        if severity != "Normal":
            print(anomaly_message)

        output_lines.append(log_entry)
        if severity != "Normal":
            output_lines.append(anomaly_message)

        total_cpu = sum(float(line.split("CPU:")[1].split("%")[0]) for line in output_lines if "allocated" in line)
        summary = f"Summary: Total CPU {total_cpu}% across {len(devices) + len(previous_cpus)} devices at {timestamp}"
        print(summary)

        with open("../output.txt", "w") as f:
            f.write("\n".join(output_lines))

if __name__ == "__main__":
    main()