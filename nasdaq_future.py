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

    holidays_datetime = [dt.datetime.strptime(date, '%Y-%m-%d') for date in holidaysUSA]

    # df_5m['Datetime'] = pd.to_datetime(df_5m["Datetime"])
    # df_5m = df_5m[df_5m["Datetime"] != "2024-03-28 08:00:00"]
    # df_5m = df_5m[~df_5m['Datetime'].dt.date.isin([holiday.date() for holiday in holidays_datetime])]
    # df_5m = df_5m[df_5m["Datetime"].dt.day_of_week != 6] # remove sundays

    df_1h = pd.read_csv('C:/Users/FSX-P/Aktienanalyse/pay/nasdaq/nq_1h_src.csv', delimiter=',')
    df_1h['Datetime'] = pd.to_datetime(df_1h["Datetime"])

    df_1h = df_1h[~df_1h['Datetime'].dt.date.isin([holiday.date() for holiday in holidays_datetime])]
    df_1h = df_1h[df_1h["Datetime"].dt.day_of_week != 6] # remove sundays

    return df_1h, None

def findMaximumBetween0800_2200(df):

    print("Finding maximum between 8:00 - 22:00")
    df_filtered = df[(df["Datetime"].dt.hour >= 2) & (df["Datetime"].dt.hour <= 15)] # only 2am (8:00) - 4pm (21:59)
    df_filtered = df_filtered.reset_index(drop=True)
    df_highFull = pd.DataFrame()

    high_day = 0
    high_index = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_highFull.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_highFull.at[i, "high_time_full"] = df_filtered.at[high_index, "Datetime"].time()
            df_highFull.at[i, "high_full"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0

        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i

    df_highFull = df_highFull.reset_index(drop=True)
    df_highFull = df_highFull.set_index("date") 

    return df_highFull

def findMaximumBetween0800_1400(df):

    print("Finding maximum between 8:00 - 14:00")
    df_filtered = df[(df["Datetime"].dt.hour >= 2) & (df["Datetime"].dt.hour <= 7)] # only 2am (8:xx) - 7am (13:xx)
    df_filtered = df_filtered.reset_index(drop=True)
    df_highEU = pd.DataFrame()

    high_day = 0
    high_index = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_highEU.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_highEU.at[i, "high_time"] = df_filtered.at[high_index, "Datetime"].time()
            df_highEU.at[i, "high"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0

        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i


    df_highEU = df_highEU.reset_index(drop=True)
    df_highEU = df_highEU.set_index("date")
    return df_highEU

def findMaximumBetween1000_2200(df):

    print("Finding maximum between 10:00 - 22:00")
    df_filtered = df[(df["Datetime"].dt.hour >= 4) & (df["Datetime"].dt.hour <= 15)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_max1000To2200 = pd.DataFrame()

    high_day = 0
    high_index = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_max1000To2200.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_max1000To2200.at[i, "val1000To2200High"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0
            high = 0
        
        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i


    df_max1000To2200 = df_max1000To2200.reset_index(drop=True)
    df_max1000To2200 = df_max1000To2200.set_index("date")
    return df_max1000To2200

def findMaximumBetween1500_2200(df):

    print("Finding maximum between 15:00 - 22:00")
    df_filtered = df[(df["Datetime"].dt.hour >= 9) & (df["Datetime"].dt.hour <= 15)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_max1500To2200 = pd.DataFrame()

    high_day = 0
    high_index = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_max1500To2200.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_max1500To2200.at[i, "val1500To2200High"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0

        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i


    df_max1500To2200 = df_max1500To2200.reset_index(drop=True)
    df_max1500To2200 = df_max1500To2200.set_index("date")
    return df_max1500To2200

def findOpenClose(df):

    print("Finding open/close for every day")
    df_filtered = df[(df["Datetime"].dt.hour == 2) | (df["Datetime"].dt.hour == 15)] # remove all but 2am (8:00) and 3pm (21:59)
    df_filtered = df_filtered.reset_index(drop=True)
    df_openClose = pd.DataFrame()

    for i, day in df_filtered.iterrows():

        if day["Datetime"].hour == 15:
            continue

        if df_filtered.at[i+1, "Datetime"].hour != 15:
            continue

        df_openClose.at[i, "date"] = day["Datetime"].date()
        df_openClose.at[i, "open"] = day["Open"]
        df_openClose.at[i, "close"] = df_filtered.at[i+1, "Close"]

        df_openClose.at[i, "2100"] = df_filtered.at[i+1, "Open"]
        df_openClose.at[i, "2100high"] = df_filtered.at[i+1, "High"]
        df_openClose.at[i, "2100low"] = df_filtered.at[i+1, "Low"]

    df_openClose = df_openClose.reset_index(drop=True)
    df_openClose = df_openClose.set_index("date")
    return df_openClose

def findSpecificValues(df):

    print("Finding 10:00 / 14:00")
    df_filtered = df[(df["Datetime"].dt.hour == 4) | (df["Datetime"].dt.hour == 8) ]
    df_filtered = df_filtered.reset_index(drop=True)
    df_specificValues = pd.DataFrame()

    index = 0

    for i, hour in df_filtered.iterrows():

        df_specificValues.at[index, "date"] = hour["Datetime"].date()

        if hour["Datetime"].hour == 4:
            df_specificValues.at[index, "val1000"] = df_filtered.at[i, "Open"]

        if hour["Datetime"].hour == 8:
            df_specificValues.at[index, "val1400"] = df_filtered.at[i, "Open"]

        

    df_specificValues = df_specificValues.reset_index(drop=True)
    df_specificValues = df_specificValues.set_index("date")
    df_filtered = None
    return df_specificValues

def find1000And1400(df):

    print("Finding 10:00 / 14:00")
    df_filtered = df[(df["Datetime"].dt.hour == 3) | (df["Datetime"].dt.hour == 8)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_11am2pm = pd.DataFrame()

    index = 0

    for i, hour in df_filtered.iterrows():

        if hour["Datetime"].hour == 8:
            index = index + 1
            continue

        df_11am2pm.at[index, "date"] = hour["Datetime"].date()
        df_11am2pm.at[index, "val1000"] = df_filtered.at[i, "Open"]
        df_11am2pm.at[index, "valCustom"] = df_filtered.at[i, "Open"]
        df_11am2pm.at[index, "val1400"] = df_filtered.at[i+1, "Open"]


    df_11am2pm = df_11am2pm.reset_index(drop=True)
    df_11am2pm = df_11am2pm.set_index("date")
    df_filtered = None
    return df_11am2pm

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

def find1500(df):
    print("Finding 15:00")
    df_filtered = df[(df["Datetime"].dt.hour == 9)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_1500 = pd.DataFrame()

    index = 0

    for i, hour in df_filtered.iterrows():
        df_1500.at[index, "date"] = hour["Datetime"].date()
        df_1500.at[index, "val1500"] = df_filtered.at[i, "Open"]
        index += 1

    df_1500 = df_1500.reset_index(drop=True)
    df_1500 = df_1500.set_index("date")
    return df_1500

def find08xxAnd09xx(df, minutesIn8xx, minutesIn9xx):

    print("Finding 08:xx / 09:xx")
    df_filtered = df[((df["Datetime"].dt.hour == 2) & (df["Datetime"].dt.minute == minutesIn8xx)) | (df["Datetime"].dt.hour == 3) & (df["Datetime"].dt.minute == minutesIn9xx)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_8am9am = pd.DataFrame()

    for i, hour in df_filtered.iterrows():

        if not i%2 == 0 or i >= len(df_filtered)-4:
            continue

        df_8am9am.at[i, "date"] = hour["Datetime"].date()
        df_8am9am.at[i, "val08xx"] = hour["Open"]
        df_8am9am.at[i, "val09xx"] = df_filtered.at[i+1, "Open"]

    df_8am9am = df_8am9am.reset_index(drop=True)
    df_8am9am = df_8am9am.set_index("date")
    df_filtered = None
    return df_8am9am

def findMaximumBetween08xx_0959(df, minutesIn8xx, minutesIn9xx):

    print("Finding maximum between 8:30 - 9:59")
    start_time = dt.datetime.strptime("02" + str(minutesIn8xx), "%H%M").time()
    end_time = dt.datetime.strptime("03" + str(minutesIn9xx), "%H%M").time()
    df["isInTimeframe"] = (df.Datetime.dt.time >= start_time) & (df.Datetime.dt.time <= end_time)
    df_filtered = df[(df["isInTimeframe"] == True)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_high9am = pd.DataFrame()

    high_day = 0
    high_index = 0
    high = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_high9am.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_high9am.at[i, "val08xxHigh_time"] = df_filtered.at[high_index, "Datetime"].time()
            df_high9am.at[i, "val08xx09xxHigh"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0
            high = 0
        
        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i

    df_high9am = df_high9am.reset_index(drop=True)
    df_high9am = df_high9am.set_index("date")
    df_filtered = None
    return df_high9am

def findLowBetween0800_1400(df):

    print("Finding low between 8:00 - 14:00")
    df_filtered = df[df["Datetime"].dt.day_of_week != 6] # remove sundays
    df_filtered = df_filtered[(df_filtered["Datetime"].dt.hour >= 2) & (df_filtered["Datetime"].dt.hour <= 7)]
    df_filtered = df_filtered[df_filtered["Datetime"] != "2024-03-28 08:00:00"]
    df_filtered = df_filtered.reset_index(drop=True)

    df_low = pd.DataFrame()

    for i, hour in df_filtered.iterrows():

        if i+1 >= len(df_filtered)-1:
            continue

        if i==0:
            low_day = hour["Low"]
            low_index = i

        low = hour["Low"]
        if low <= low_day:
            low_day = low
            low_index = i

        if hour["Datetime"].date() < df_filtered.at[i+1, "Datetime"].date():
            df_low.at[i, "date"] = df_filtered.at[low_index, "Datetime"].date()
            df_low.at[i, "low_time"] = df_filtered.at[low_index, "Datetime"].time()
            df_low.at[i, "low"] = df_filtered.at[low_index, "Low"]
            low_day = df_filtered.at[i+1, "Low"]
            low_index = i+1
            low = 0

    df_low = df_low.reset_index(drop=True)
    df_low = df_low.set_index("date")
    df_filtered = None
    return df_low

def findLowBetween0800_2200(df):

    print("Finding low between 8:00 - 22:00")
    df_filtered = df[df["Datetime"].dt.day_of_week != 6] # remove sundays
    df_filtered = df_filtered[(df_filtered["Datetime"].dt.hour >= 2) & (df_filtered["Datetime"].dt.hour <= 15)]
    df_filtered = df_filtered.reset_index(drop=True)

    df_low = pd.DataFrame()

    for i, hour in df_filtered.iterrows():

        if i+1 >= len(df_filtered)-1:
            continue

        if i==0:
            lowest_day = hour["Low"]
            lowest_index = i

        low_temp = hour["Low"]
        if low_temp <= lowest_day:
            lowest_day = low_temp
            lowest_index = i

        if hour["Datetime"].date() < df_filtered.at[i+1, "Datetime"].date():
            df_low.at[i, "date"] = df_filtered.at[lowest_index, "Datetime"].date()
            df_low.at[i, "low_full"] = df_filtered.at[lowest_index, "Low"]
            lowest_day = df_filtered.at[i+1, "Low"]
            lowest_index = i+1

    df_low = df_low.reset_index(drop=True)
    df_low = df_low.set_index("date")
    return df_low

def downloadNasdaq_1h():

    print("Downloading '1h' data...")

    nasdaq_future = "NQ=F"

    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -300)

    ticker = yf.Ticker(nasdaq_future).history(start=dateStart, end=dateToday, interval="1h")

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')
    df = tickerNoTimezone[["Open", "High", "Low", "Close", "Volume"]]

    print("Writing to .csv file...")
    df.to_csv('C:/Users/FSX-P/Aktienanalyse/pay/nasdaq/nq_1h_temp.csv', index=True)

def downloadNasdaq_5m():

    print("Downloading '5m' data...")

    nasdaq_future = "NQ=F"

    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -57)
    dateEnd = nextWorkdayAfterDays(dateToday, -10)

    ticker = yf.Ticker(nasdaq_future).history(start=dateStart, end=dateToday, interval="5m")

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')

    print("Writing to excel file...")

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='nasdaq_future_data_15m_temp', index=True)

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

        if day["Datetime"].day_of_week == 4 or i+1 >= len(df_monFri):
            continue

        mon = day

        if df_monFri.at[i+1, "Datetime"].day_of_week == 4:
            fri = df_monFri.iloc[i+1]
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

def processFutureAnalyticsData():
        
    df_1h, df_5m = loadDataframes()

    df_openClose = findOpenClose(df_1h)
    df_highFull = findMaximumBetween0800_2200(df_1h)
    df_highEU = findMaximumBetween0800_1400(df_1h)
    df_lowFull = findLowBetween0800_2200(df_1h)
    df_lowEU = findLowBetween0800_1400(df_1h)
    df_10To22High = findMaximumBetween1000_2200(df_1h)
    df_15To22High = findMaximumBetween1500_2200(df_1h)
    df_10am2pm = find1000And1400(df_1h)
    df_9am = find0900(df_1h)
    df_3pm = find1500(df_1h)
    # df_8am9am = find08xxAnd09xx(df_5m, 30, 55)
    # df_high9am = findMaximumBetween08xx_0959(df_5m, 35, 55)
    df_monFriPerf = findMondayToFridayPerformance(df_1h)



    # join all dataframes
    print("Joining dataframes")
    df_joined = df_openClose.join(df_highFull)

    df_joined = df_highEU.join(df_joined)
    df_joined = df_joined.join(df_10am2pm)
    df_joined = df_joined.join(df_10To22High)
    df_joined = df_joined.join(df_15To22High)
    df_joined = df_joined.join(df_lowEU)
    df_joined = df_joined.join(df_lowFull)
    df_joined = df_joined.join(df_9am)
    df_joined = df_joined.join(df_3pm)
    # df_joined = df_joined.join(df_8am9am, how="left")
    # df_joined = df_joined.join(df_high9am, how="left")
    df_joined = df_joined.join(df_monFriPerf, how="left")

    df_joined = df_joined.reset_index()

    df_joined = findPositiveStreak(df_joined)
    df_joined = findNegativeStreak(df_joined)

    # final sheet
    print("Final sheet")
    df_result = pd.DataFrame()

    for i, day in df_joined.iterrows():

        if i-1<0 or i-2<0 or i+1>len(df_joined)-1:
            continue

        # EU
        openEU_0 = day["open"]
        highEU_0 = day["high"]
        lowEU_0 = day["low"]
        val1000 = day["val1000"]
        val1400 = day["val1400"]
        # val08xx = day["val08xx"]
        # val09xx = day["val09xx"]
        val08xx = 1
        val09xx = 1
        # val08xx09xxHigh = day["val08xx09xxHigh"]
        val08xx09xxHigh = 1
        val1000To2200High = day["val1000To2200High"]
        val1500To2200High = day["val1500To2200High"]
        val0900 = day["val0900"]
        valCustom = day["valCustom"]
        val1500 = day["val1500"]

        openEU_plus1 = df_joined.at[i+1, "open"]
        val1400_plus1 = df_joined.at[i+1, "val1400"]
        openEU_minus1 = df_joined.at[i-1, "open"]
        openEU_minus2 = df_joined.at[i-2, "open"]



        # US
        high_0 = day["high_full"]
        low_0 = day["low_full"]
        close_0 = day["close"]
        high_time = day["high_time_full"]

        high_minus1 = df_joined.at[i-1, "high_full"]
        high_plus1 = df_joined.at[i+1, "high_full"]
        close_plus1 = df_joined.at[i+1, "close"]
        close_minus1 = df_joined.at[i-1, "close"]
        close_minus2 = df_joined.at[i-2, "close"]



        # special
        # _2100open = day["2100"]
        # _2100high = day["2100high"]
        # _2100low = day["2100low"]
        _2100open = 1
        _2100high = 1
        _2100low = 1
        percentBuffer = 0.0
        _monFriPerf = day["monFriPerf"]
        # va08xxHighTime = day["val08xxHigh_time"]
        va08xxHighTime = 1



        df_result.at[i, "date"] = day["date"]
        df_result.at[i, "_openToClose"] = -round((openEU_0 - close_0) / openEU_0, 4)
        df_result.at[i, "_openToHighEU"] = -round((openEU_0 - highEU_0) / openEU_0, 4)
        df_result.at[i, "_openToHigh"] = -round((openEU_0 - high_0) / openEU_0, 4)
        df_result.at[i, "_1000To2pm"] = -round((val1000 - val1400) / val1000, 4)
        df_result.at[i, "_1000ToHigh"] = -round((val1000 - val1000To2200High) / val1000, 4)
        df_result.at[i, "_openTo10am"] = -round((openEU_0 - val1000) / openEU_0, 4)
        df_result.at[i, "_openNextDayToHighNextDay"] = -round((openEU_plus1 - high_plus1) / openEU_plus1, 4)
        df_result.at[i, "_openNextDayToCloseNextDay"] = -round((openEU_plus1 - close_plus1) / openEU_plus1, 4)
        df_result.at[i, "_closeToCloseNextDay"] = -round((close_0 - close_plus1) / close_0, 4)
        df_result.at[i, "_openTo08xx"] = -round((openEU_0 - val08xx) / openEU_0, 4)
        df_result.at[i, "_08xxToHigh"] = -round((val08xx - high_0) / val08xx, 4)
        df_result.at[i, "_08xxToCustom"] = -round((val08xx - val1400) / val08xx, 4)
        df_result.at[i, "_0900ToHigh"] = -round((val0900 - highEU_0) / val0900, 4)
        df_result.at[i, "_openToCustom"] = -round((openEU_0 - val0900) / openEU_0, 4)
        df_result.at[i, "_customToCustom"] = -round((close_minus1 - openEU_0) / close_minus1, 4)
        df_result.at[i, "_customToHigh"] = -round((val0900 - highEU_0) / val0900, 4)
        df_result.at[i, "_openToLowEU"] = -round((openEU_0 - lowEU_0) / openEU_0, 4)
        df_result.at[i, "_openToLow"] = -round((openEU_0 - low_0) / openEU_0, 4)
        df_result.at[i, "_openTo1500"] = -round((openEU_0 - val1500) / openEU_0, 4)

        df_result.at[i, "_t-1_closeTo0900"] = -round((close_minus1 - val0900) / close_minus1, 4)
        df_result.at[i, "_t-1_openToClose"] = -round((openEU_minus1 - close_minus1) / openEU_minus1, 4)
        df_result.at[i, "_t-1_openToHigh"] = -round((openEU_minus1 - high_minus1) / openEU_minus1, 4)
        df_result.at[i, "_t-1_openToClose"] = -round((openEU_minus1 - close_minus1) / openEU_minus1, 4)
        df_result.at[i, "_t-1_closeTo10am"] = -round((close_minus1 - val1000) / close_minus1, 4)
        df_result.at[i, "_t-1_closeTo08xx"] = -round((close_minus1 - val08xx) / close_minus1, 4)
        df_result.at[i, "_t-1_closeTo1500"] = -round((close_minus1 - val1500) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToOpen"] = -round((close_minus1 - openEU_0) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToHigh"] = -round((close_minus1 - high_0) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToClose"] = -round((close_minus1 - close_0) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToCustom"] = -round((close_minus1 - valCustom) / close_minus1, 4)
        df_result.at[i, "_t-1_closeTo1500"] = -round((close_minus1 - val1500) / close_minus1, 4)#
        df_result.at[i, "_t-2_openToClose"] = -round((openEU_minus2 - close_minus2) / openEU_minus2, 4)
        df_result.at[i, "_t-2_closeToClose"] = -round((close_minus2 - close_minus1) / close_minus2, 4)
        df_result.at[i, "_t-2_closeToOpen"] = -round((close_minus2 - openEU_0) / close_minus2, 4)

        df_result.at[i, "_closeToOpenNextDay"] = -round((close_0 - openEU_plus1) / close_0, 4)
        df_result.at[i, "_closeToMaxNextDay"] = -round((close_0 - high_plus1) / close_0, 4)
        df_result.at[i, "_nextDayOpenToNextDay1359"] = -round((openEU_plus1 - val1400_plus1) / openEU_plus1, 4)
        df_result.at[i, "_21ToHigh"] = -round((_2100open - _2100high) / _2100open, 4)
        df_result.at[i, "_21ToClose"] = -round((_2100open - close_0) / _2100open, 4)
        df_result.at[i, "_21ToLow"] = -round((_2100open - _2100low) / _2100open, 4)
        df_result.at[i, "_highTo21"] = -round((high_0 - _2100open) / high_0, 4)
        df_result.at[i, "_highToClose"] = -round((high_0 - close_0) / high_0, 4)
        df_result.at[i, "_08xxTo09xx"] = -round((val08xx - val09xx) / val08xx, 4)
        df_result.at[i, "_08xxTo09xxHigh"] = -round((val08xx - val08xx09xxHigh) / val08xx, 4)
        df_result.at[i, "_monFriPerf"] = _monFriPerf
        df_result.at[i, "_1500To2200High"] = -round((val1500 - val1500To2200High) / val1500, 4)

        df_result.at[i, "dayOfWeek"] = day["date"].isocalendar()[2]
        df_result.at[i, "val08xxTo09xxHighTime"] = va08xxHighTime
        df_result.at[i, "val_highTime"] = high_time

        # aus Sicht "heute": wie viele vergangene Tage mit positive/negatvie run?
        df_result.at[i, "ctc_positiveRun"] = df_joined.at[i-2, "ctc_positiveRun"]
        df_result.at[i, "ctc_negativeRun"] = df_joined.at[i-2, "ctc_negativeRun"]

        df_result.at[i, "bool_isHighEUTodayDay>=ClosePreviousDay"] = greaterEqual(highEU_0, close_minus1)
        df_result.at[i, "bool_isHighUSTodayDay>=ClosePreviousDay"] = greaterEqual(high_0, close_minus1)
        df_result.at[i, "bool_is08xxValReachedBetweem10am10pm"] = greaterEqual(val1000To2200High, val08xx)
        df_result.at[i, "bool_isCloseValReachedBetween10am10pm"] = greaterEqual(val1000To2200High, close_minus1*(1-percentBuffer*0.01))
        df_result.at[i, "bool_isHigh15002200>=ClosePreviousDay"] = greaterEqual(val1500To2200High, close_minus1)

    print("[Saving to .csv]")
    df_result.to_csv("C:/Users/FSX-P/Aktienanalyse/pay/nasdaq/nq_1h_result.csv", index=False, sep=";", decimal=",")

def processPayData():
    print("Loading dataframe")

    df = pd.read_csv('C:/Users/FSX-P/Aktienanalyse/pay/nasdaq/nq_5m.csv', delimiter=';')
    df.columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df['Datetime'] = df['Date'] + ' ' + df['Time']
    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%d/%m/%Y %H:%M:%S')
    df = df[["Datetime", "Open", "High", "Low", "Close", "Volume"]]

    df_new = pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])

    for i, fiveMinuteSlot in df.iterrows():

        # only pay attention to full hours
        if fiveMinuteSlot["Datetime"].time().minute != 0:
            continue

        dateTimeStart = fiveMinuteSlot["Datetime"]
        dateTimeEnd = dateTimeStart + timedelta(minutes=55)

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
    df_new.to_csv('C:/Users/FSX-P/Aktienanalyse/pay/nasdaq/nq_1h_processPayData.csv', index=False)

    # holidays_datetime = [dt.datetime.strptime(date, '%Y-%m-%d') for date in holidaysUSA]
    # df_5m = df_5m[~df_5m['Datetime'].dt.date.isin([holiday.date() for holiday in holidays_datetime])]

if __name__ == "__main__":
    # downloadNasdaq_5m()
    # downloadNasdaq_1h()
    processFutureAnalyticsData()
    # processPayData()