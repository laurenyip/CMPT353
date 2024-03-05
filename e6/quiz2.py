import pandas as pd
from scipy.stats import ttest_ind

# Assuming your DataFrame is named 'logs'
# Extract the data for implementation A and B
implementation_a = logs[logs['Implementation'] == 'A']
implementation_b = logs[logs['Implementation'] == 'B']

# Calculate the error rates for each implementation
error_rate_a = implementation_a['Errors'] / implementation_a['Requests']
error_rate_b = implementation_b['Errors'] / implementation_b['Requests']

# Perform the t-test
t_stat, p_value = ttest_ind(error_rate_a, error_rate_b, equal_var=True)

# Print the p-value
print("p-value:", p_value)


#part 2

# Calculate the number of crashes for each implementation
crashes_a = implementation_a['Crashes'].sum()
crashes_b = implementation_b['Crashes'].sum()

# Perform the chi-square test
from scipy.stats import chi2_contingency

observed = [[crashes_a, crashes_b]]
chi2, p_value, _, _ = chi2_contingency(observed)

# Print the p-value
print("p-value:", p_value)