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

def downloadRUSSELData():

    russel2000_future = "RTY=F"

    dateToday = dt.date.today()
    dateYesterday = nextWorkdayAfterDays(dateToday, -720)
    dateTomorrow = nextWorkdayAfterDays(dateToday, 1)     

    ticker = yf.Ticker(russel2000_future).history(start=dateYesterday, end=dateToday, interval="1h")
    print(ticker)

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='RUSSEL2000_future', index=True)

def processRUSSELData():
        
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="RUSSEL2000_future")

    df['Datetime'] = pd.to_datetime(df["Datetime"])
    df = df[df["Datetime"] != "2024-03-28 08:00:00"]
    df = df[df["Datetime"].dt.day_of_week != 6] # remove sundays

    df_result = pd.DataFrame()



    # find maximum value between 8:xx - 13:xx 

    df_filtered = df[(df["Datetime"].dt.hour >= 2) & (df["Datetime"].dt.hour <= 7)] # only 2am (8:xx) - 7am (13:xx)
    df_filtered = df_filtered.reset_index(drop=True)

    high_day = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_result.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_result.at[i, "high_time"] = df_filtered.at[high_index, "Datetime"].time()
            df_result.at[i, "high"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0
            high = 0
        
        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i


    df_result = df_result.reset_index(drop=True)



    # find maximum value between 8:xx - 21:59 

    df_filtered = df[(df["Datetime"].dt.hour >= 2) & (df["Datetime"].dt.hour <= 15)] # only 2am (8:00) - 4pm (21:59)
    df_filtered = df_filtered.reset_index(drop=True)

    df_high_full = pd.DataFrame()

    high_day = 0
    high_index = 0
    high = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_high_full.at[i, "high_time_full"] = df_filtered.at[high_index, "Datetime"]
            df_high_full.at[i, "high_full"] = df_filtered.at[high_index, "High"]
            high_day = 0
            high_index = 0
            high = 0
        
        high = hour["High"]
        if high > high_day:
            high_day = high
            high_index = i

    df_high_full = df_high_full.reset_index(drop=True)

    for i, hday in df_result.iterrows():

        for c, cday in df_high_full.iterrows():

            if (hday["date"] == cday["high_time_full"].date()):
                df_result.at[i, "high_time_full"] = cday["high_time_full"].time()
                df_result.at[i, "high_full"] = cday["high_full"]

    df_result = df_result.dropna()
    df_result = df_result.reset_index(drop=True)



    # find lowest value between 8:xx - 13:xx

    # df_filtered = df[df["Datetime"].dt.day_of_week != 6] # remove sundays
    # df_filtered = df_filtered[(df_filtered["Datetime"].dt.hour >= 2) & (df_filtered["Datetime"].dt.hour <= 7)] # only 2am (8:xx) - 7am (13:xx)
    # df_filtered = df_filtered[df_filtered["Datetime"] != "2024-03-28 08:00:00"]
    # df_filtered = df_filtered.reset_index(drop=True)

    # df_low = pd.DataFrame()

    # low_day = 0

    # for i, hour in df_filtered.iterrows():

    #     if (i-1 < 0):
    #         continue

    #     if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
    #         df_low.at[i, "low_date"] = df_filtered.at[low_index, "Datetime"].date()
    #         df_low.at[i, "low_time"] = df_filtered.at[low_index, "Datetime"].time()
    #         df_low.at[i, "low"] = df_filtered.at[low_index, "Low"]
    #         low_day = 0
    #         low_index = 0
    #         low = 0
        
    #     low = hour["Low"]
    #     if low < low_day:
    #         low_day = low
    #         low_index = i

    # df_low = df_low.reset_index(drop=True)

    


    # find open/close values for every day

    df_close = df[(df["Datetime"].dt.hour == 2) | (df["Datetime"].dt.hour == 15)] # remove all but 2am (8:00) and 3pm (21:59)
    df_close = df_close.reset_index(drop=True)

    for i, hday in df_result.iterrows():

        for c, cday in df_close.iterrows():

            if(cday["Datetime"].hour == 15):
                continue

            if (hday["date"] == cday["Datetime"].date()):
                df_result.at[i, "open"] = cday["Open"]
                df_result.at[i, "close"] = df_close.at[c+1, "Close"]
                df_result.at[i, "2100"] = df_close.at[c+1, "Open"]
                df_result.at[i, "2100high"] = df_close.at[c+1, "High"]
                df_result.at[i, "2100low"] = df_close.at[c+1, "Low"]

    df_result = df_result.dropna()
    df_result = df_result.reset_index(drop=True)




    # find "euroclose" for everday (13:59)

    df_filtered = df[(df["Datetime"].dt.hour == 7)] # 7am (13:xx)
    df_filtered = df_filtered.reset_index(drop=True)

    for i, hour in df_filtered.iterrows():
        df_result.at[i, "1359"] = hour["Close"]
   
    

    # calculate openToClose / closeToMaxNextDay / closeToOpen / openTo1350

    for i, day in df_result.iterrows():


        if i-1<0 or i-2<0 or i-3<0 or i+1>len(df_result)-1:
            continue

        #EU
        openEU_0 = day["open"]    
        highEU_0 = day["high"]
        closeEU_0 = day["1359"]

        openEU_plus1 = df_result.at[i+1, "open"]
        highEU_plus1 = df_result.at[i+1, "high"]
        closeEU_plus1 = df_result.at[i+1, "1359"]


        #US
        high_0 = day["high_full"]
        close_0 = day["close"]

        high_plus1 = df_result.at[i+1, "high"]
        close_minus1 = df_result.at[i-1, "close"]


        #special
        _2100open = day["2100"]
        _2100high = day["2100high"]
        _2100low = day["2100low"]
        
        df_result.at[i, "_openToClose"] = -round((openEU_0 - close_0) / openEU_0, 4)
        df_result.at[i, "_openNextDayToHighNextDay"] = -round((openEU_plus1 - highEU_plus1) / openEU_plus1, 4)
        df_result.at[i, "_closeToCloseToday"] = -round((close_minus1 - close_0) / close_minus1, 4)
        df_result.at[i, "_t-1_openToClose"] = df_result.at[i-1, "_openToClose"]
        df_result.at[i, "_t-2_openToClose"] = df_result.at[i-2, "_openToClose"]
        df_result.at[i, "_t-3_openToClose"] = df_result.at[i-3, "_openToClose"]
        df_result.at[i, "_t-1_closeToClose"] = df_result.at[i-1, "_closeToCloseToday"]
        df_result.at[i, "_t-1_closeToOpen"] = -round((close_minus1 - openEU_0) / close_minus1, 4)
        df_result.at[i, "_closeToOpenNextDay"] = -round((close_0 - openEU_plus1) / close_0, 4)
        df_result.at[i, "_closeToMaxNextDay"] = -round((close_0 - highEU_plus1) / close_0, 4)
        df_result.at[i, "_openTo1359"] = -round((openEU_0 - closeEU_0) / openEU_0, 4)
        df_result.at[i, "_nextDayOpenToNextDay1359"] = -round((openEU_plus1 - closeEU_plus1) / openEU_plus1, 4)
        df_result.at[i, "_21ToHigh"] = -round((_2100open - _2100high) / _2100open, 4)
        df_result.at[i, "_21ToClose"] = -round((_2100open - close_0) / _2100open, 4)
        df_result.at[i, "_21ToLow"] = -round((_2100open - _2100low) / _2100open, 4)
        df_result.at[i, "_highTo21"] = -round((high_0 - _2100open) / high_0, 4)
        df_result.at[i, "_highToClose"] = -round((high_0 - close_0) / high_0, 4)
        df_result.at[i, "bool_isHighEUTodayDay>=ClosePreviousDay"] = "yes" if highEU_0 >= close_minus1 else "no"
        df_result.at[i, "bool_isHighUSTodayDay>=ClosePreviousDay"] = "yes" if high_0 >= close_minus1 else "no"
        df_result.at[i, "dayOfWeek"] = day["date"].isocalendar()[2]

        # df_result.at[i, "maxToClose"] = -round((high_0 - close_0) / high_0, 4)

    df_result = df_result.dropna()

    print(df_result)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df_result.to_excel(writer, sheet_name='RUSSEL2000_future_script', index=False)

#downloadRUSSELData()
processRUSSELData()