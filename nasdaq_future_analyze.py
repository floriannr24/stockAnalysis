import numpy as np
import pandas as pd


def loadDataframes():
    print("[Loading dataframes]")

    df_1h = pd.read_csv('C:/Users/FSX-P/Aktienanalyse/pay/nasdaq/nq_1h_result.csv', delimiter=';', decimal=",")
    df_1h['date'] = pd.to_datetime(df_1h["date"])
    df_1h = df_1h[df_1h["date"].dt.year >= 2020]
    df_1h = df_1h.reset_index()

    return df_1h

def analyzeFutureAnalyticsData():
        
    df = loadDataframes()

    def find_optimal_range(df, target_percentage=0.85, min_rows=30):
        """
        Finds the optimal range for '_t-2_closeToClose' to achieve a target percentage
        of rows where '_openToHigh' >= 0.002, with a minimum row count.

        Args:
            df (pd.DataFrame): Input DataFrame with 'date', '_t-2_closeToClose', and '_openToHigh' columns.
            target_percentage (float): Target percentage (e.g., 0.9 for 90%).
            min_rows (int): Minimum number of rows required to meet the criteria.

        Returns:
            tuple: A tuple containing the optimal range (min_val, max_val) or None if not found.
        """

        best_range = None
        best_count = 0

        _t2_values = sorted(df['_t-2_closeToClose'].unique())

        for i in range(len(_t2_values)):
            for j in range(i, len(_t2_values)):
                min_val = _t2_values[i]
                max_val = _t2_values[j]

                filtered_df = df[
                    (df['_t-2_closeToClose'] >= min_val) & (df['_t-2_closeToClose'] <= max_val)
                    ]

                if len(filtered_df) == 0:
                    continue

                filtered_positive = filtered_df[filtered_df['_openToHigh'] >= 0.005]

                if len(filtered_df) > 0:
                    percentage = len(filtered_positive) / len(filtered_df)

                    if percentage >= target_percentage and len(filtered_df) >= min_rows:
                        if len(filtered_positive) > best_count:
                            best_count = len(filtered_positive)
                            best_range = (min_val, max_val)

        return best_range

    optimal_range = find_optimal_range(df)
    if optimal_range:
        print(f"Optimal range for '_t-2_closeToClose': {optimal_range}")
    else:
        print("No optimal range found that meets the criteria.")

if __name__ == "__main__":
    analyzeFutureAnalyticsData()
