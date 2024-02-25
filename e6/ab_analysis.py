import sys
import pandas as pd
from scipy.stats import chi2_contingency
from scipy.stats import mannwhitneyu, chi2_contingency


OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)
  
    
    
def ab_testing_analysis(data):
    
    #filter to seprate odd and even uid 
    #src: https://www.geeksforgeeks.org/split-pandas-dataframe-by-column-value/
    #filter to separate odd and even uid 
    filter_even = data['uid'] % 2 == 0
    filter_odd = data['uid'] % 2 == 1
    data_even = data[filter_even].reset_index().drop(['index'], axis=1)
    data_odd = data[filter_odd].reset_index().drop(['index'], axis=1)

    
    #separate 0 and not 0 searches, for even and odd cases s
    even_search_zero = data_even[data_even['search_count']==0]
    even_search_not_zero = data_even[data_even['search_count']!=0] 
    odd_search_zero = data_odd[data_odd['search_count']==0]
    odd_search_not_zero = data_odd[data_odd['search_count']!=0] 
    
    # Create contingency table
    contingency_table = [[len(even_search_not_zero),len(even_search_zero)],[len(odd_search_not_zero),len(odd_search_zero)]]
    # get the values of chi2, p, dof, ex with chi testing
    # A/B Testing for all users
    _, more_users_p, _, _ = chi2_contingency(contingency_table)
    # Mann-Whitney U test for more_searches_p
    more_searches_p = mannwhitneyu(data_even['search_count'],data_odd['search_count']).pvalue
    
    # filter instructors
    instructor_even = data_even[data_even['is_instructor']==True]
    instructor_odd = data_odd[data_odd['is_instructor']==True]
    
    # separate 0 and not 0 searches for even and odd uid instructors
    even_instructor_zero = instructor_even[instructor_even['search_count']==0]
    even_instructor_not_zero = instructor_even[instructor_even['search_count']!=0] 
    odd_instructor_zero = instructor_odd[instructor_odd['search_count']==0]
    odd_instructor_not_zero = instructor_odd[instructor_odd['search_count']!=0]



    # Create contingency table for instructors
    instr_contingency = [[len(even_instructor_not_zero),len(even_instructor_zero)],[len(odd_instructor_not_zero),len(odd_instructor_zero)]]
    # A/B Testing for instructors
    _, more_instr_p, _, _ = chi2_contingency(instr_contingency)
    
   
   
    
    more_instr_searches_p = mannwhitneyu(instructor_even['search_count'],instructor_odd['search_count']).pvalue

    

    return more_users_p, more_instr_p, more_searches_p, more_instr_searches_p



def main():
    searchdata_file = sys.argv[1]
    
    # Read and load data
    data = pd.read_json(searchdata_file, orient='records', lines=True)

    # Perform A/B Testing
    more_users_p, more_instr_p, more_searches_p, more_instr_searches_p = ab_testing_analysis(data)
   
    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=more_users_p,
        more_searches_p=more_searches_p,
        more_instr_p=more_instr_p,
        more_instr_searches_p=more_instr_searches_p,
    ))


if __name__ == '__main__':
    main()
