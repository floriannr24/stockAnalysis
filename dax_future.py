from datetime import timedelta
import datetime as dt

import numpy as np
import yfinance as yf
import pandas as pd


holidaysUSA = ["2022-09-05", "2022-11-24",
               "2023-01-02", "2023-01-16", "2023-02-20", "2023-04-07", "2023-05-29", "2023-06-19", "2023-07-03", "2023-07-04", "2023-09-04", "2023-11-23","2023-11-24", "2023-11-25",
               "2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29", "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02", "2024-11-28", "2024-11-25",
               "2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17", "2025-04-18", "2025-06-26", "2025-07-03", "2025-07-04", "2025-09-01", "2025-11-27", "2025-11-28", "2025-12-24", "2025-12-25"]

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

    df_30m = pd.read_csv('C:/Users/FSX-P/Aktienanalyse/pay/dax/dax_30m_src.csv', delimiter=',')
    df_30m['Datetime'] = pd.to_datetime(df_30m["Datetime"])
    df_30m = df_30m[df_30m["Datetime"].dt.day_of_week != 6] # remove sundays

    return df_30m

def findOpen(df):
    print("Finding open for every day")
    df_filtered = df[(df["Datetime"].dt.hour == 2) & (df["Datetime"].dt.minute == 0)] # remove all but 2am (8:00)
    df_filtered = df_filtered.reset_index(drop=True)
    df_open = pd.DataFrame()

    for i, thirtyMindTimeslot in df_filtered.iterrows():
        df_open.at[i, "date"] = thirtyMindTimeslot["Datetime"].date()
        df_open.at[i, "open"] = thirtyMindTimeslot["Open"]

    df_open = df_open.reset_index(drop=True)
    df_open = df_open.set_index("date")
    return df_open

def findClose(df):
    print("Finding close for every day")
    df_filtered = df[(df["Datetime"].dt.hour == 15) & (df["Datetime"].dt.minute == 30)]  # remove all but 10pm (22:00)
    df_filtered = df_filtered.reset_index(drop=True)
    df_close = pd.DataFrame()

    for i, thirtyMinTimeslot in df_filtered.iterrows():
        df_close.at[i, "date"] = thirtyMinTimeslot["Datetime"].date()
        df_close.at[i, "close"] = thirtyMinTimeslot["Close"]

    df_close = df_close.reset_index(drop=True)
    df_close = df_close.set_index("date")
    return df_close

def find0900(df):

    print("Finding 09:00")
    df_filtered = df[(df["Datetime"].dt.hour == 3)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_9am = pd.DataFrame()

    index = 0

    for i, hour in df_filtered.iterrows():
        
        df_9am.at[index, "date"] = hour["Datetime"].date()
        df_9am.at[index, "val0900"] = df_filtered.at[i, "Open"]
        index += 1

    df_9am = df_9am.reset_index(drop=True)
    df_9am = df_9am.set_index("date")
    df_filtered = None
    return df_9am

def find1730(df):
    print("Finding 17:30")
    df_filtered = df[(df["Datetime"].dt.hour == 10) & (df["Datetime"].dt.minute == 0)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_1730Close = pd.DataFrame()

    for i, thirtyMinuteSlot in df_filtered.iterrows():
        df_1730Close.at[i, "date"] = thirtyMinuteSlot["Datetime"].date()
        df_1730Close.at[i, "val1730"] = df_filtered.at[i, "Close"]

    df_1730Close = df_1730Close.reset_index(drop=True)
    df_1730Close = df_1730Close.set_index("date")
    return df_1730Close

def findMaximumBetween1730_2200(df):
    print("Finding maximum between 17:30 - 22:00")
    df_filtered = df[((df["Datetime"].dt.hour >= 10) & (df["Datetime"].dt.minute == 30)) & ((df["Datetime"].dt.hour <= 15) & (df["Datetime"].dt.minute == 30) )]
    df_filtered = df_filtered.reset_index(drop=True)
    df_1730To2200High = pd.DataFrame()

    high_day = 0
    high_index = 0

    for i, hour in df_filtered.iterrows():

        if i - 1 < 0:
            continue

        if (hour["Datetime"].date() > df_filtered.at[i - 1, "Datetime"].date()):
            df_1730To2200High.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_1730To2200High.at[i, "1730To22high"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0

        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i

    df_1730To2200High = df_1730To2200High.reset_index(drop=True)
    df_1730To2200High = df_1730To2200High.set_index("date")
    return df_1730To2200High

def greaterEqual(high, valToReach):
    if not high or not valToReach or np.isnan(high) or np.isnan(valToReach):
        return None
    else:
        return "yes" if high >= valToReach else "no"

def smallerEqual(low, valToReach):
    if not low or not valToReach or np.isnan(low) or np.isnan(valToReach):
        return None
    else:
        return "yes" if low <= valToReach else "no"

def findPositiveStreak(df):

    print("Finding positive streak")

    streak = 1

    for i, day in df.iterrows():

        if i == 0:
            continue

        if day["close"] > df.at[i-1, "close"]:
            df.at[i, "ctc_positiveRun"] = streak
            streak = streak + 1
        else: streak = 1

    return df

def findNegativeStreak(df):

    print("Finding negative streak")

    streak = 1

    for i, day in df.iterrows():

        if i == 0:
            continue

        if day["close"] < df.at[i-1, "close"]:
            df.at[i, "ctc_negativeRun"] = streak
            streak = streak + 1
        else: streak = 1

    return df

def processDaxFutureData():

    df_30m = loadDataframes()

    df_open = findOpen(df_30m)
    df_close = findClose(df_30m)
    df_val1730 = find1730(df_30m)
    df_1730To22High = findMaximumBetween1730_2200(df_30m)

    print("Joining dataframes")
    df_joined = df_close.join(df_open)
    df_joined = df_joined.join(df_val1730)
    df_joined = df_joined.join(df_1730To22High)

    df_joined = df_joined.reset_index()

    df_joined = findPositiveStreak(df_joined)
    df_joined = findNegativeStreak(df_joined)

    print("Final sheet")
    df_result = pd.DataFrame()

    for i, day in df_joined.iterrows():

        if i - 1 < 0 or i - 2 < 0:
            continue

        open = day["open"]
        close = day["close"]
        val1730 = day["val1730"]
        val1730To22High = day["1730To22high"]

        close_minus1 = df_joined.at[i-1, "close"]
        val1730_minus1 = df_joined.at[i-1, "val1730"]

        close_minus2 = df_joined.at[i-2, "close"]

        df_result.at[i, "date"] = day["date"]
        df_result.at[i, "_1730To2200High"] = -round((val1730 - val1730To22High) / val1730, 4)
        df_result.at[i, "_1730To2200"] = -round((val1730 - close) / val1730, 4)
        df_result.at[i, "_openToClose"] = -round((open - close) / open, 4)
        df_result.at[i, "_t-1_closeToOpen"] = -round((close_minus1 - open) / close_minus2, 4)
        df_result.at[i, "_t-1_closeToClose"] = -round((close_minus1 - close) / close_minus1, 4)
        df_result.at[i, "_t-1_1730To1730"] = -round((val1730_minus1 - val1730) / val1730_minus1, 4)
        df_result.at[i, "_t-2_closeToClose"] = -round((close_minus2 - close_minus1) / close_minus2, 4)
        df_result.at[i, "_t-2_closeTo1730"] = -round((close_minus2 - val1730) / close_minus2, 4)

        # aus Sicht "heute": wie viele vergangene Tage mit positive/negatvie run?
        df_result.at[i, "ctc_positiveRun"] = df_joined.at[i-2, "ctc_positiveRun"]
        df_result.at[i, "ctc_negativeRun"] = df_joined.at[i-2, "ctc_negativeRun"]

    print("[Saving to .csv]")
    df_result.to_csv("C:/Users/FSX-P/Aktienanalyse/pay/dax/dax_30m_result.csv", index=False, sep=";", decimal=",")

def processDaxPayData():
    print("Loading dataframe")

    df = pd.read_csv('C:/Users/FSX-P/Aktienanalyse/pay/dax/original/dax-5m.csv', delimiter=';')
    df.columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df['Datetime'] = df['Date'] + ' ' + df['Time']
    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%d/%m/%Y %H:%M:%S')
    df = df[["Datetime", "Open", "High", "Low", "Close", "Volume"]]

    df_new = pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])

    for i, fiveMinuteSlot in df.iterrows():

        # only pay attention to half hours
        if fiveMinuteSlot["Datetime"].time().minute != 0 and fiveMinuteSlot["Datetime"].time().minute != 30:
            continue

        dateTimeStart = fiveMinuteSlot["Datetime"]
        dateTimeEnd = dateTimeStart + timedelta(minutes=25)

        df_hour = df[(df['Datetime'] >= dateTimeStart) & (df['Datetime'] <= dateTimeEnd)]

        open = df_hour.iloc[0, 1]
        high = df_hour["High"].max()
        low = df_hour["Low"].min()
        close = df_hour.iloc[-1, 4]
        volumeSum = df_hour["Volume"].sum()
        datetime = df_hour.iloc[0, 0]

        df_temp = pd.DataFrame({
            "Datetime": [datetime],
            "Open": [open],
            "High": [high],
            "Low": [low],
            "Close": [close],
            "Volume": [volumeSum]
        })

        df_new = pd.concat([df_new, df_temp], ignore_index=True)

    df_new = df_new[df_new["Datetime"].dt.day_of_week != 6] # remove sundays
    df_new["Datetime"] = df_new["Datetime"] + dt.timedelta(hours=1)
    df_new = df_new.reset_index(drop=True)

    print("Saving dataframe")
    df_new.to_csv('C:/Users/FSX-P/Aktienanalyse/pay/dax/dax_30m.csv', index=False)

    # holidays_datetime = [dt.datetime.strptime(date, '%Y-%m-%d') for date in holidaysUSA]
    # df_5m = df_5m[~df_5m['Datetime'].dt.date.isin([holiday.date() for holiday in holidays_datetime])]

if __name__ == "__main__":
    # downloadNasdaq_5m()
    # downloadNasdaq_1h()
    processDaxFutureData()
    # processDaxPayData()