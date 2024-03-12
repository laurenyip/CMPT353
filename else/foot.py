import pandas as pd
import matplotlib.pyplot as plt

def compare_and_plot(file1, file2, output_path='comparison_plots'):
    # Read the CSV files
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Assuming both CSV files have the same structure and columns
    columns_to_compare = ['time', 'gFx', 'gFy', 'gFz', 'TgF']

    # Check if the values in the specified columns are almost equal
    comparison_result = df1[columns_to_compare].equals(df2[columns_to_compare])

    # Print the result of the comparison
    print(f"The two CSV files are {'equal' if comparison_result else 'not equal'}.")

    # Create a folder for saving plots
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Plot the data from both files for visual comparison and save as PNG
    for col in columns_to_compare[1:]:  # Skip 'time' for x-axis
        plt.figure()
        plt.plot(df1['time'], df1[col], label='File 1')
        plt.plot(df2['time'], df2[col], label='File 2')
        plt.xlabel('Time')
        plt.ylabel(col)
        plt.legend()
        plt.title(f'{col} Comparison')

        # Save the plot as PNG
        plt.savefig(os.path.join(output_path, f'{col}_comparison.png'))
        plt.close()

if __name__ == "__main__":
    import sys
    import os

    if len(sys.argv) != 3:
        print("Usage: python3 foot.py file1.csv file2.csv")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    compare_and_plot(file1, file2)
