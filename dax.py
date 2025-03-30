from datetime import timedelta
import datetime as dt

import numpy as np
import yfinance as yf
import pandas as pd

# holidaysUSA = ["2022-09-05", "2022-11-24",
#                "2023-01-02", "2023-01-16", "2023-02-20", "2023-04-07", "2023-05-29", "2023-06-19", "2023-07-03",
#                "2023-07-04", "2023-09-04", "2023-11-23", "2023-11-24", "2023-11-25",
#                "2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29", "2024-05-27", "2024-06-19", "2024-07-04",
#                "2024-09-02", "2024-11-28", "2024-11-25",
#                "2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17", "2025-04-18", "2025-06-26", "2025-07-03",
#                "2025-07-04", "2025-09-01", "2025-11-27", "2025-11-28", "2025-12-24", "2025-12-25"]


def nextWorkdayAfterDays(date, days):
    if days == 0:
        return date

    newDate = date + timedelta(days=days)

    if days < 0:

        # if saturday
        if newDate.isoweekday() == 6:
            newDate = newDate - timedelta(days=1)

        # if sunday
        if newDate.isoweekday() == 7:
            newDate = newDate - timedelta(days=2)

    if days > 0:

        # if saturday
        if newDate.isoweekday() == 6:
            newDate = newDate + timedelta(days=2)

        # if sunday
        if newDate.isoweekday() == 7:
            newDate = newDate + timedelta(days=1)

    return newDate


def loadDataframes():
    print("[Loading dataframes]")

    df_1d = pd.read_csv('C:/Users/FSX-P/Aktienanalyse/dax/dax_1d_src.csv', delimiter=',', decimal=".")
    df_1d['Datetime'] = pd.to_datetime(df_1d["Date"])
    df_1d = df_1d[df_1d["Datetime"].dt.day_of_week != 6]  # remove sundays

    return df_1d

def downloadDax_1d():
    print("Downloading '1d' data...")

    dax = "^GDAXI"

    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -2000)

    ticker = yf.Ticker(dax).history(start=dateStart, end=dateToday, interval="1d")

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')
    df = tickerNoTimezone[["Open", "High", "Low", "Close", "Volume"]]

    print("Writing to .csv file...")
    df.to_csv('C:/Users/FSX-P/Aktienanalyse/dax/dax_1d_temp.csv', index=True)

def findPositiveStreak(df):
    print("Finding positive streak")

    streak = 1

    for i, day in df.iterrows():

        if i == 0:
            continue

        if day["close"] > df.at[i - 1, "close"]:
            df.at[i, "ctc_positiveRun"] = streak
            streak = streak + 1
        else:
            streak = 1

    return df

def findNegativeStreak(df):
    print("Finding negative streak")

    streak = 1

    for i, day in df.iterrows():

        if i == 0:
            continue

        if day["close"] < df.at[i - 1, "close"]:
            df.at[i, "ctc_negativeRun"] = streak
            streak = streak + 1
        else:
            streak = 1

    return df

def greaterEqual(high, valToReach):
    if not high or not valToReach or np.isnan(high) or np.isnan(valToReach):
        return None
    else:
        return "yes" if high >= valToReach else "no"

def findMondayToFridayPerformance(df):
    print("Finding Mon - Fri performance")

    df_monFri = df[(df["Datetime"].dt.day_of_week == 0) & (df["Datetime"].dt.hour == 2) |
                   (df["Datetime"].dt.day_of_week == 4) & (df["Datetime"].dt.hour == 15)]

    df_monFri = df_monFri.reset_index(drop=True)
    df_monFriPerf = pd.DataFrame()

    # monFri performance
    for i, day in df_monFri.iterrows():

        mon = None
        fri = None

        if day["Datetime"].day_of_week == 4 or i + 1 >= len(df_monFri):
            continue

        mon = day

        if df_monFri.at[i + 1, "Datetime"].day_of_week == 4:
            fri = df_monFri.iloc[i + 1]
        else:
            continue

        if not mon.empty and not fri.empty:
            openMon = mon["Open"]
            closeFri = fri["Close"]

            monFriPerf = -round((openMon - closeFri) / openMon, 4)

            df_monFriPerf.at[i, "date"] = mon["Datetime"].date()
            df_monFriPerf.at[i, "monFriPerf"] = monFriPerf

    # high in week
    # df_filtered = df[(df["Datetime"].dt.hour >= 2) & (df["Datetime"].dt.hour <= 15)]
    # df_filtered = df_filtered.reset_index(drop=True)
    # df_highWeek = pd.DataFrame();

    # high_day = 0

    # for i, hour in df_filtered.iterrows():

    #     if (i-1 < 0):
    #         continue

    #     mon = None
    #     fri = None

    #     if day["Datetime"].day_of_week == 4 or i+1 >= len(df_monFri):
    #         continue

    #     mon = day

    #     if df_monFri.at[i+1, "Datetime"].day_of_week == 4:
    #         fri = df_monFri.iloc[i+1]
    #     else:
    #         continue

    #     if not mon.empty and not fri.empty:

    #     if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
    #         df_highEU.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
    #         df_highEU.at[i, "high_time"] = df_filtered.at[high_index, "Datetime"].time()
    #         df_highEU.at[i, "high"] = df_filtered.at[high_index, "High"]
    #         high_day = 0
    #         high_index = 0
    #         high = 0

    #     high = hour["High"]
    #     if high > high_day:
    #         high_day = high
    #         high_index = i

    df_monFriPerf = df_monFriPerf.reset_index(drop=True)
    df_monFriPerf = df_monFriPerf.set_index("date")

    return df_monFriPerf


def processDaxData_1d():
    df_1d = loadDataframes()

    # df_monFriPerf = findMondayToFridayPerformance(df_1h)
    # df_joined = df_joined.join(df_monFriPerf, how="left")

    df_joined = df_1d.reset_index()

    df_joined = findPositiveStreak(df_joined)
    df_joined = findNegativeStreak(df_joined)

    # final sheet
    print("Final sheet")
    df_result = pd.DataFrame()

    for i, day in df_joined.iterrows():

        if i - 1 < 0 or i - 2 < 0:
            continue

        open = day["Open"]
        high = day["High"]
        low = day["Low"]
        close = day["Close"]

        open_minus1 = df_joined.at[i-1, "Open"]
        high_minus1 = df_joined.at[i-1, "High"]
        low_minus1 = df_joined.at[i-1, "Low"]
        close_minus1 = df_joined.at[i-1, "Close"]

        open_minus2 = df_joined.at[i-2, "Open"]
        high_minus2 = df_joined.at[i-2, "High"]
        low_minus2 = df_joined.at[i-2, "Low"]
        close_minus2 = df_joined.at[i-2, "Close"]

        df_result.at[i, "date"] = day["Datetime"]
        df_result.at[i, "_openToHigh"] = -round((open - high) / open, 4)
        df_result.at[i, "_openToLow"] = -round((open - low) / open, 4)
        df_result.at[i, "_openToClose"] = -round((open - close) / open, 4)
        df_result.at[i, "_t-1_closeToClose"] = -round((close_minus1 - close) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToHigh"] = -round((close_minus1 - high) / close_minus2, 4)
        df_result.at[i, "_t-1_closeToOpen"] = -round((close_minus1 - open) / close_minus2, 4)
        df_result.at[i, "_t-2_closeToClose"] = -round((close_minus2 - close_minus1) / close_minus2, 4)

        # aus Sicht "heute": wie viele vergangene Tage mit positive/negatvie run?
        df_result.at[i, "ctc_positiveRun"] = df_joined.at[i-2, "ctc_positiveRun"]
        df_result.at[i, "ctc_negativeRun"] = df_joined.at[i-2, "ctc_negativeRun"]

    print("[Saving to .csv]")
    df_result.to_csv("C:/Users/FSX-P/Aktienanalyse/dax/dax_1d_result.csv", index=False, sep=";", decimal=",")


if __name__ == "__main__":
    # downloadDax_1d()
    processDaxData_1d()