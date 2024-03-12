import sys
from scipy import stats
import pandas as pd
import numpy as np


#python3 reddit_weekends.py reddit-counts.json.gz

OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    

    #student's t-test - get a p-value
    
    
    # Read data from the provided file
    counts = pd.read_json(sys.argv[1], lines=True)
    
    # Filter data for the specified conditions
    counts = counts[((counts['date'].dt.year == 2012) | (counts['date'].dt.year == 2013)) & (counts['subreddit'] == 'canada')].reset_index()
   
    # Separate data into weekdays and weekends
    weekdays_data = counts[counts['date'].dt.dayofweek < 5].reset_index(drop = True)
  
    weekends_data = counts[counts['date'].dt.dayofweek >= 5].reset_index(drop = True)
  

    # get the comment count
    weekday_comments = weekdays_data['comment_count']
    weekend_comments = weekends_data['comment_count']
    
    # Check if there are enough samples for statistical tests
    if len(weekdays_data) < 8 or len(weekends_data) < 8:
        print("Insufficient data for statistical tests.")
        sys.exit(1)
    
    # Perform a t-test
   
    
    _, initial_ttest_p = stats.ttest_ind(weekday_comments, weekend_comments)
    
    # Check for normality using normaltest
    _, initial_weekday_normality_p = stats.normaltest(weekday_comments)
    _, initial_weekend_normality_p = stats.normaltest(weekend_comments)
    
    # Check for equal variances using Levene's test
    _, initial_levene_p = stats.levene(weekday_comments, weekend_comments)
   

    # Fix 1: Transforming data
    transformed_weekdays_data = np.sqrt(weekday_comments) 
    transformed_weekends_data = np.sqrt(weekend_comments)

    _, transformed_weekday_normality_p = stats.normaltest(transformed_weekdays_data)
    _, transformed_weekend_normality_p = stats.normaltest(transformed_weekends_data)

    _, transformed_levene_p = stats.levene(transformed_weekdays_data, transformed_weekends_data)

   # Fix 2: Central Limit Theorem
# Function to get year and week
    def year_week(df):
        return df.isocalendar()[0], df.isocalendar()[1]
    grouped_weekdays = weekdays_data.groupby(weekdays_data['date'].apply(year_week))['comment_count'].mean().reset_index()
    grouped_weekends = weekends_data.groupby(weekends_data['date'].apply(year_week))['comment_count'].mean().reset_index()
    
    
    _, weekly_weekday_normality_p = stats.normaltest(grouped_weekdays['comment_count'])
    _, weekly_weekend_normality_p = stats.normaltest(grouped_weekends['comment_count'])

    _, weekly_levene_p = stats.levene(grouped_weekdays['comment_count'], grouped_weekends['comment_count'])
    _, weekly_ttest_p = stats.ttest_ind(grouped_weekdays['comment_count'], grouped_weekends['comment_count'])


    # Fix 3: Non-parametric test (Mann–Whitney U-test)
    _, utest_p = stats.mannwhitneyu(counts[counts['date'].dt.weekday < 5]['comment_count'],
                                counts[counts['date'].dt.weekday >= 5]['comment_count'],
                                alternative='two-sided')


    # ...

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=initial_ttest_p,
        initial_weekday_normality_p=initial_weekday_normality_p,
        initial_weekend_normality_p=initial_weekend_normality_p,
        initial_levene_p=initial_levene_p,
        transformed_weekday_normality_p=transformed_weekday_normality_p,
        transformed_weekend_normality_p=transformed_weekend_normality_p,
        transformed_levene_p=transformed_levene_p,
        weekly_weekday_normality_p=weekly_weekday_normality_p,
        weekly_weekend_normality_p=weekly_weekend_normality_p,
        weekly_levene_p=weekly_levene_p,
        weekly_ttest_p=weekly_ttest_p,
        utest_p=utest_p,
    ))



if __name__ == '__main__':
    main()
