from datetime import timedelta
import datetime as dt
import yfinance as yf
import pandas as pd


holidaysUSA = ["2022-09-05", "2022-11-24",
               "2023-01-16", "2023-02-20", "2023-04-07", "2023-05-29", "2023-06-19", "2023-07-04", "2023-09-04", "2023-11-23",
               "2024-01-15", "2024-02-19", "2024-03-29", "2024-05-27", "2024-06-19", "2024-07-04"]

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

    df_1h = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="NASDAQ_future")
    df_5m = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="NASDAQ_future_15m")


    df_1h['Datetime'] = pd.to_datetime(df_1h["Datetime"])
    df_1h = df_1h[df_1h["Datetime"] != "2024-03-28 08:00:00"]
    df_1h = df_1h[df_1h["Datetime"].dt.day_of_week != 6] # remove sundays

    df_5m['Datetime'] = pd.to_datetime(df_5m["Datetime"])
    df_5m = df_5m[df_5m["Datetime"] != "2024-03-28 08:00:00"]
    df_5m = df_5m[df_5m["Datetime"].dt.day_of_week != 6] # remove sundays

    return df_1h, df_5m

def findMaximumBetween0800_2159(df):

    print("Finding maximum between 8:xx - 21:59")
    df_filtered = df[(df["Datetime"].dt.hour >= 2) & (df["Datetime"].dt.hour <= 15)] # only 2am (8:00) - 4pm (21:59)
    df_filtered = df_filtered.reset_index(drop=True)
    df_highFull = pd.DataFrame()

    high_day = 0
    high_index = 0
    high = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_highFull.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_highFull.at[i, "high_time_full"] = df_filtered.at[high_index, "Datetime"].time()
            df_highFull.at[i, "high_full"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0
            high = 0
        
        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i

    df_highFull = df_highFull.reset_index(drop=True)
    df_highFull = df_highFull.set_index("date") 
    df_filtered = None

    return df_highFull

def findMaximumBetween0800_1359(df):

    print("Finding maximum between 8:xx - 13:xx")
    df_filtered = df[(df["Datetime"].dt.hour >= 2) & (df["Datetime"].dt.hour <= 7)] # only 2am (8:xx) - 7am (13:xx)
    df_filtered = df_filtered.reset_index(drop=True)
    df_highEU = pd.DataFrame();

    high_day = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_highEU.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_highEU.at[i, "high_time"] = df_filtered.at[high_index, "Datetime"].time()
            df_highEU.at[i, "high"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0
            high = 0
        
        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i


    df_highEU = df_highEU.reset_index(drop=True)
    df_highEU = df_highEU.set_index("date")
    df_filtered = None
    return df_highEU

def findMaximumBetween1000_2159(df):

    print("Finding maximum between 10:xx - 13:xx")
    df_filtered = df[(df["Datetime"].dt.hour >= 4) & (df["Datetime"].dt.hour <= 15)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_max10To10pm = pd.DataFrame();

    high_day = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_max10To10pm.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_max10To10pm.at[i, "val10amTo10pmHigh"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0
            high = 0
        
        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i


    df_max10To10pm = df_max10To10pm.reset_index(drop=True)
    df_max10To10pm = df_max10To10pm.set_index("date")
    df_filtered = None
    return df_max10To10pm

def findOpenClose(df):

    print("Finding open/close for every day")
    df_filtered = df[(df["Datetime"].dt.hour == 2) | (df["Datetime"].dt.hour == 15)] # remove all but 2am (8:00) and 3pm (21:59)
    df_filtered = df_filtered.reset_index(drop=True)
    df_openClose = pd.DataFrame()

    for i, day in df_filtered.iterrows():

        if(day["Datetime"].hour == 15):
            continue

        df_openClose.at[i, "date"] = day["Datetime"].date()
        df_openClose.at[i, "open"] = day["Open"]
        df_openClose.at[i, "08low"] = day["Low"]
        df_openClose.at[i, "close"] = df_filtered.at[i+1, "Close"]
        df_openClose.at[i, "2100"] = df_filtered.at[i+1, "Open"]
        df_openClose.at[i, "2100high"] = df_filtered.at[i+1, "High"]
        df_openClose.at[i, "2100low"] = df_filtered.at[i+1, "Low"]

    df_openClose = df_openClose.reset_index(drop=True)
    df_openClose = df_openClose.set_index("date")
    df_filtered = None
    return df_openClose

def find1000And14000(df):

    print("Finding 09:59 / 13:59")
    df_filtered = df[(df["Datetime"].dt.hour == 3) | (df["Datetime"].dt.hour == 7)] # 7am (13:xx) // 3am (9:xx)
    df_filtered = df_filtered.reset_index(drop=True)
    df_10am2pm = pd.DataFrame()

    index = 0

    for i, hour in df_filtered.iterrows():

        if hour["Datetime"].hour == 7:
            index = index + 1
            continue
        
        df_10am2pm.at[index, "date"] = hour["Datetime"].date()
        df_10am2pm.at[index, "0959"] = df_filtered.at[i, "Close"]
        df_10am2pm.at[index, "1359"] = df_filtered.at[i+1, "Close"]

    df_10am2pm = df_10am2pm.reset_index(drop=True)
    df_10am2pm = df_10am2pm.set_index("date")
    df_filtered = None
    return df_10am2pm

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

def findMaximumBetween0900_0959(df):

    print("Finding maximum between 9:00 - 9:59")
    df_filtered = df[(df["Datetime"].dt.hour == 3)]
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
            df_high9am.at[i, "time_high"] = df_filtered.at[high_index, "Datetime"].time()
            df_high9am.at[i, "val09xxHigh"] = df_filtered.at[high_index, "High"]
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

def findLowBetween800_1300(df):

    df_filtered = df[df["Datetime"].dt.day_of_week != 6] # remove sundays
    df_filtered = df_filtered[(df_filtered["Datetime"].dt.hour >= 2) & (df_filtered["Datetime"].dt.hour <= 7)] # only 2am (8:xx) - 7am (13:xx)
    df_filtered = df_filtered[df_filtered["Datetime"] != "2024-03-28 08:00:00"]
    df_filtered = df_filtered.reset_index(drop=True)

    df_low = pd.DataFrame()

    low_day = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_low.at[i, "low_date"] = df_filtered.at[low_index, "Datetime"].date()
            df_low.at[i, "low_time"] = df_filtered.at[low_index, "Datetime"].time()
            df_low.at[i, "low"] = df_filtered.at[low_index, "Low"]
            low_day = 0
            low_index = 0
            low = 0
        
        low = hour["Low"]
        if low < low_day:
            low_day = low
            low_index = i

    df_low = df_low.reset_index(drop=True)

def downloadNasdaq_1h():

    nasdaq_future = "NQ=F"

    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -720)     

    ticker = yf.Ticker(nasdaq_future).history(start=dateStart, end=dateToday, interval="1h")
    print(ticker)

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='nasdaq_future_data_temp', index=True)

def downloadNasdaq_5m():

    nasdaq_future = "NQ=F"

    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -59)   

    ticker = yf.Ticker(nasdaq_future).history(start=dateStart, end=dateToday, interval="5m")
    print(ticker)

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='nasdaq_future_data_15m_temp', index=True)

def processFutureAnalyticsData():
        
    df_1h, df_5m = loadDataframes()

    df_highFull = findMaximumBetween0800_2159(df_1h)
    df_highEU = findMaximumBetween0800_1359(df_1h)
    df_max10To10pm = findMaximumBetween1000_2159(df_1h)
    df_openClose = findOpenClose(df_1h)
    df_10am2pm = find1000And14000(df_1h)
    df_8am9am = find08xxAnd09xx(df_5m, 30, 55)
    df_high9am = findMaximumBetween0900_0959(df_5m)




    # join all dataframes
    print("Joining dataframes")
    df_joined = df_highEU.join(df_highFull)
    df_joined = df_joined.join(df_10am2pm)
    df_joined = df_joined.join(df_openClose)
    df_joined = df_joined.join(df_max10To10pm)
    df_joined = df_joined.join(df_8am9am, how="left")
    df_joined = df_joined.join(df_high9am, how="left")

    df_joined = df_joined.reset_index()




    # final sheet
    print("Final sheet")
    df_result = pd.DataFrame()

    for i, day in df_joined.iterrows():

        if i-1<0 or i-2<0 or i-3<0 or i+1>len(df_joined)-1:
            continue

        # EU
        openEU_0 = day["open"]    
        highEU_0 = day["high"]
        val1359 = day["1359"]
        val0959 = day["0959"]
        val08xxLow = day["08low"]
        val08xx = day["val08xx"]    
        val09xx = day["val09xx"]
        val09xxHigh = day["val09xxHigh"]
        val10amTo10pmHigh = day["val10amTo10pmHigh"]

        openEU_plus1 = df_joined.at[i+1, "open"]
        highEU_plus1 = df_joined.at[i+1, "high"]
        val1359_plus1 = df_joined.at[i+1, "1359"]
        openEU_minus1 = df_joined.at[i-1, "open"]
        openEU_minus2 = df_joined.at[i-2, "open"]
        openEU_minus3 = df_joined.at[i-3, "open"]


        # US
        high_0 = day["high_full"]
        close_0 = day["close"]

        close_minus1 = df_joined.at[i-1, "close"]
        close_minus2 = df_joined.at[i-2, "close"]
        close_minus3 = df_joined.at[i-3, "close"]

        # special
        _2100open = day["2100"]
        _2100high = day["2100high"]
        _2100low = day["2100low"]


        df_result.at[i, "date"] = day["date"]
        df_result.at[i, "_openToClose"] = -round((openEU_0 - close_0) / openEU_0, 4)
        df_result.at[i, "_openToHighEU"] = -round((openEU_0 - highEU_0) / openEU_0, 4)
        df_result.at[i, "_openToHigh_nasdaq"] = -round((openEU_0 - high_0) / openEU_0, 4)
        df_result.at[i, "_08xxLowTo10am"] = -round((val08xxLow - val0959) / val08xxLow, 4)
        df_result.at[i, "_openTo10am"] = -round((openEU_0 - val0959) / openEU_0, 4)
        df_result.at[i, "_openNextDayToHighNextDay"] = -round((openEU_plus1 - highEU_plus1) / openEU_plus1, 4)
        df_result.at[i, "_closeToCloseToday"] = -round((close_minus1 - close_0) / close_minus1, 4)
        df_result.at[i, "_t-1_openToClose"] = -round((openEU_minus1 - close_minus1) / openEU_minus1, 4)
        df_result.at[i, "_t-2_openToClose"] = -round((openEU_minus2 - close_minus2) / openEU_minus2, 4)
        df_result.at[i, "_t-3_openToClose"] = -round((openEU_minus3 - close_minus3) / openEU_minus3, 4)
        df_result.at[i, "_t-1_closeToClose"] = -round((close_minus1 - close_0) / close_minus1, 4)
        df_result.at[i, "_t-2_closeToClose"] = -round((close_minus2 - close_minus1) / close_minus2, 4)
        df_result.at[i, "_t-1_closeToOpen"] = -round((close_minus1 - openEU_0) / close_minus1, 4)
        df_result.at[i, "_closeToOpenNextDay"] = -round((close_0 - openEU_plus1) / close_0, 4)
        df_result.at[i, "_closeToMaxNextDay"] = -round((close_0 - highEU_plus1) / close_0, 4)
        df_result.at[i, "_openTo1359"] = -round((openEU_0 - val1359) / openEU_0, 4)
        df_result.at[i, "_nextDayOpenToNextDay1359"] = -round((openEU_plus1 - val1359_plus1) / openEU_plus1, 4)
        df_result.at[i, "_21ToHigh"] = -round((_2100open - _2100high) / _2100open, 4)
        df_result.at[i, "_21ToClose"] = -round((_2100open - close_0) / _2100open, 4)
        df_result.at[i, "_21ToLow"] = -round((_2100open - _2100low) / _2100open, 4)
        df_result.at[i, "_highTo21"] = -round((high_0 - _2100open) / high_0, 4)
        df_result.at[i, "_highToClose"] = -round((high_0 - close_0) / high_0, 4)
        df_result.at[i, "bool_isHighEUTodayDay>=ClosePreviousDay"] = "yes" if highEU_0 >= close_minus1 else "no"
        df_result.at[i, "bool_isHighUSTodayDay>=ClosePreviousDay"] = "yes" if high_0 >= close_minus1 else "no"
        df_result.at[i, "dayOfWeek"] = day["date"].isocalendar()[2]
        df_result.at[i, "_08xxTo09xx"] = -round((val08xx - val09xx) / val08xx, 4)
        df_result.at[i, "_08xxTo09xxHigh"] = -round((val08xx - val09xxHigh) / val08xx, 4)
        df_result.at[i, "bool_is08xxValReachedAfter10am"] = "yes" if val10amTo10pmHigh >= val08xx else "no"




    print("Writing to Excel")
    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df_result.to_excel(writer, sheet_name='NASDAQ_future_script', index=False)
    

# downloadNasdaq_5m()
# downloadNasdaq_1h()
processFutureAnalyticsData()