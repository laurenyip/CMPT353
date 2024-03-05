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
    reddit_counts = sys.argv[1]
    print(reddit_counts)
    #student's t-test - get a p-value
    
    # Read data from the provided file
    counts = pd.read_json(reddit_counts, lines=True)

    # Filter data for the specified conditions
    reddit_data = counts[(counts['date'].dt.year.isin([2012, 2013])) & (counts['subreddit'] == '/r/canada')]
    print(reddit_data)
    # Separate data into weekdays and weekends
    weekdays_data = reddit_data[reddit_data['date'].dt.weekday < 5]['comment_count']
    weekends_data = reddit_data[reddit_data['date'].dt.weekday >= 5]['comment_count']

    print(weekdays_data)
    print(weekends_data)

    # Perform a t-test
    tr_stat, p_val = stats.ttest_ind(weekdays_data, weekends_data)

    print(tr_stat, p_val)

    """
    # Check if there are enough samples for statistical tests
    if len(weekdays_data) < 8 or len(weekends_data) < 8:
        print("Insufficient data for statistical tests.")
        sys.exit(1)

    # Check for normality using normaltest
    _, initial_weekday_normality_p = stats.normaltest(weekdays_data)
    _, initial_weekend_normality_p = stats.normaltest(weekends_data)

    # Check for equal variances using Levene's test
    _, initial_levene_p = stats.levene(weekdays_data, weekends_data)

    # Fix 1: Transforming data
    transformed_weekdays_data = np.sqrt(weekdays_data)  # Choose an appropriate transformation
    transformed_weekends_data = np.sqrt(weekends_data)

    _, transformed_weekday_normality_p = stats.normaltest(transformed_weekdays_data)
    _, transformed_weekend_normality_p = stats.normaltest(transformed_weekends_data)

    _, transformed_levene_p = stats.levene(transformed_weekdays_data, transformed_weekends_data)

    # Fix 2: Central Limit Theorem
    reddit_data['year_week'] = reddit_data['date'].dt.isocalendar().loc[:, ['year', 'week']].astype(str).agg('-'.join, axis=1)

    weekly_means = reddit_data.groupby('year_week')['comment_count'].mean()

    _, weekly_weekday_normality_p = stats.normaltest(weekly_means[reddit_data['date'].dt.weekday < 5])
    _, weekly_weekend_normality_p = stats.normaltest(weekly_means[reddit_data['date'].dt.weekday >= 5])

    _, weekly_levene_p = stats.levene(weekly_means[reddit_data['date'].dt.weekday < 5], weekly_means[reddit_data['date'].dt.weekday >= 5])

    _, weekly_ttest_p = stats.ttest_ind(weekly_means[reddit_data['date'].dt.weekday < 5], weekly_means[reddit_data['date'].dt.weekday >= 5])

    # Fix 3: Non-parametric test (Mann–Whitney U-test)
    _, utest_p = stats.mannwhitneyu(reddit_data[reddit_data['date'].dt.weekday < 5]['comment_count'],
                                    reddit_data[reddit_data['date'].dt.weekday >= 5]['comment_count'],
                                    alternative='two-sided')

    # Print the results
    print(f"Student's T-Test p-value: {initial_ttest_p}")
    print(f"Weekday normaltest p-value: {initial_weekday_normality_p}")
    print(f"Weekend normaltest p-value: {initial_weekend_normality_p}")
    print(f"Levene's test p-value: {initial_levene_p}")
    print(f"\nFix 1:")
    print(f"Transformed Weekday normaltest p-value: {transformed_weekday_normality_p}")
    print(f"Transformed Weekend normaltest p-value: {transformed_weekend_normality_p}")
    print(f"Transformed Levene's test p-value: {transformed_levene_p}")
    print(f"\nFix 2:")
    print(f"Weekly Weekday normaltest p-value: {weekly_weekday_normality_p}")
    print(f"Weekly Weekend normaltest p-value: {weekly_weekend_normality_p}")
    print(f"Weekly Levene's test p-value: {weekly_levene_p}")
    print(f"Weekly T-Test p-value: {weekly_ttest_p}")
    print(f"\nFix 3:")
    print(f"Mann–Whitney U-Test p-value: {utest_p}")
    
    # ...

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=0,
        initial_weekday_normality_p=0,
        initial_weekend_normality_p=0,
        initial_levene_p=0,
        transformed_weekday_normality_p=0,
        transformed_weekend_normality_p=0,
        transformed_levene_p=0,
        weekly_weekday_normality_p=0,
        weekly_weekend_normality_p=0,
        weekly_levene_p=0,
        weekly_ttest_p=0,
        utest_p=0,
    ))
    """

if __name__ == '__main__':
    main()
