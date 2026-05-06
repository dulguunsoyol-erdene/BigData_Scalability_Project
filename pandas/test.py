### CODE TO CHECK THE FUNCTIONALITY OF THE PANDAS CODE

import pandas as pd
import numpy as np

# Read data file
df = pd.read_csv("data/1.2M_churn_data.csv") #100MB dataset
#df = pd.read_csv("data/5.6M_churn_data.csv") #512MB dataset
#df = pd.read_csv("data/11.2M_churn_data.csv") #1GB dataset

print([df.columns])
