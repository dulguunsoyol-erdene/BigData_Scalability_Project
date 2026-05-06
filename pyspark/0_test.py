### CODE TO CHECK THE FUNCTIONALITY OF THE PYSPARK CODE

import psutil
import time

# Get CPU usage (first call often returns 0.0, so call it again after a short interval for an accurate value)
psutil.cpu_percent(interval=1) 
cpu_usage = psutil.cpu_percent(interval=1) # Measures over the last second
print(f"CPU Usage: {cpu_usage}%")

# Get memory usage as a percentage
memory_percent = psutil.virtual_memory().percent
print(f"Memory Usage: {memory_percent}%")

# Get detailed memory information
memory_info = psutil.virtual_memory()
print(f"Total Memory: {round(memory_info.total / (1024**3), 2)} GB")
print(f"Used Memory: {round(memory_info.used / (1024**3), 2)} GB")
print(f"Available Memory: {round(memory_info.available / (1024**3), 2)} GB")
