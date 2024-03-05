import time
import pandas as pd
import numpy as np
from implementations import all_implementations

# Create a DataFrame with all sorting functions as columns and 100 rows
data = pd.DataFrame(columns=[sort.__name__ for sort in all_implementations], index=np.arange(100))

# Run each sorting implementation an equal number of times
num_runs = 10

for i in range(100):
    random_array = np.random.randint(-1000, 1000, size=1000)
    for sort in all_implementations:
        
        for _ in range(num_runs):
            st = time.time()
            res = sort(random_array)
            en = time.time()
            data.loc[i, sort.__name__] = en - st
        



# Save the DataFrame to a CSV file
data.to_csv('data.csv', index=False)

