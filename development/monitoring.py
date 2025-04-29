import psutil
import csv
import time

class ResourceMonitor:
    def __init__(self):
        self.metrics = []
    
    def start_monitoring(self):
        while True:
            self.metrics.append((
                time.time(),
                psutil.cpu_percent(),
                psutil.virtual_memory().percent
            ))
            time.sleep(1)

    def save_report(self, filename="usage.csv"):
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "CPU%", "Memory%"])
            writer.writerows(self.metrics)

# Test this phase
print("\nTesting resource monitoring...")
monitor = ResourceMonitor()
# Run in separate thread for real usage
for _ in range(3):
    monitor.start_monitoring()
    time.sleep(1)
monitor.save_report()
print("Saved monitoring data")