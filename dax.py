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

def downloadAnalyticsData():

    print("Downloading data")
    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -300)

    ticker = yf.Ticker("^GDAXI").history(start=dateStart, end=dateToday, interval="1h")

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')
    
    print("Writing to excel file...")
    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='DAX_917_future_data', index=True)
    
def loadDataframes():

    df_1h = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="DAX_917_future_data")

    df_1h['Datetime'] = pd.to_datetime(df_1h["Datetime"])
    df_1h = df_1h[df_1h["Datetime"].dt.day_of_week != 6] # remove sundays

    return df_1h

def findOpenClose(df):

    print("Finding open/close for every day")
    df_filtered = df[(df["Datetime"].dt.hour == 9) | (df["Datetime"].dt.hour == 17)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_openClose = pd.DataFrame()

    for i, day in df_filtered.iterrows():

        if i+1 > len(df_filtered)-1:
            continue

        if(day["Datetime"].hour == 17):
            continue

        df_openClose.at[i, "date"] = day["Datetime"].date()
        df_openClose.at[i, "open"] = day["Open"]
        df_openClose.at[i, "close"] = df_filtered.at[i+1, "Close"]

    df_openClose = df_openClose.reset_index(drop=True)
    df_openClose = df_openClose.set_index("date")
    df_filtered = None

    return df_openClose

def findMaximumBetween0900_1400(df):

    print("Finding maximum between 9:00 - 14:00")
    df_filtered = df[(df["Datetime"].dt.hour >= 9) & (df["Datetime"].dt.hour <= 13)]
    df_filtered = df_filtered.reset_index(drop=True)
    df_highEU = pd.DataFrame();

    high_day = 0

    for i, hour in df_filtered.iterrows():

        if (i-1 < 0):
            continue

        if (hour["Datetime"].date() > df_filtered.at[i-1, "Datetime"].date()):
            df_highEU.at[i, "date"] = df_filtered.at[high_index, "Datetime"].date()
            df_highEU.at[i, "highEU_time"] = df_filtered.at[high_index, "Datetime"].time()
            df_highEU.at[i, "highEU"] = df_filtered.at[high_index, "High"]
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

def findLowBetween0900_1400(df):
    
    print("Finding low between 9:00 - 14:00")
    df_filtered = df[df["Datetime"].dt.day_of_week != 6] # remove sundays
    df_filtered = df_filtered[(df_filtered["Datetime"].dt.hour >= 9) & (df_filtered["Datetime"].dt.hour <= 13)]
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
            df_low.at[i, "lowEU_time"] = df_filtered.at[low_index, "Datetime"].time()
            df_low.at[i, "lowEU"] = df_filtered.at[low_index, "Low"]
            low_day = df_filtered.at[i+1, "Low"]
            low_index = i+1
            low = 0

    df_low = df_low.reset_index(drop=True)
    df_low = df_low.set_index("date")
    df_filtered = None
    return df_low

def processAnalyticsData_DAX():

    df_1h = loadDataframes()

    df_openClose = findOpenClose(df_1h)
    df_high9am2pm = findMaximumBetween0900_1400(df_1h)
    df_low9am2pm = findLowBetween0900_1400(df_1h)

    # join all dataframes
    print("Joining dataframes")
    df_joined = df_openClose
    df_joined = df_joined.join(df_high9am2pm)
    df_joined = df_joined.join(df_low9am2pm)

    df_joined = df_joined.reset_index()




    # final sheet
    print("Final sheet")
    df_result = pd.DataFrame()

    for i, day in df_joined.iterrows():

        if i+1 > len(df_joined)-1 or i-2 < 0:
            continue

        open_0 = day["open"] 
        close_0 = day["close"]   
        highEU_0 = day["highEU"]
        lowEU_0 = day["lowEU"]


        open_plus1 = df_joined.at[i+1, "open"]
        close_plus1 = df_joined.at[i+1, "close"]
        highEU_plus1 = df_joined.at[i+1, "highEU"]
        lowEU_plus1 = df_joined.at[i+1, "lowEU"]

        close_minus1 = df_joined.at[i-1, "close"]
        open_minus1 = df_joined.at[i-1, "open"]

        close_minus2 = df_joined.at[i-2, "close"]



        df_result.at[i, "date"] = day["date"]
        df_result.at[i, "_openToHighEU"] = -round((open_0 - highEU_0) / open_0, 4)
        df_result.at[i, "_openToLowEU"] = -round((open_0 - lowEU_0) / open_0, 4)
        df_result.at[i, "_openToClose"] = -round((open_0 - close_0) / open_0, 4)
        df_result.at[i, "_closeToOpen"] = -round((close_0 - open_plus1) / close_0, 4)
        df_result.at[i, "_closeToHighEU"] = -round((close_0 - highEU_plus1) / close_0, 4)


        df_result.at[i, "_t+1_openToHighEU"] = -round((open_plus1 - highEU_plus1) / open_plus1, 4)
        df_result.at[i, "_t+1_openToLowEU"] = -round((open_plus1 - lowEU_plus1) / open_plus1, 4)
        df_result.at[i, "_t+1_openToClose"] = -round((open_plus1 - close_plus1) / open_plus1, 4)
        df_result.at[i, "_t-1_closeToOpen"] = -round((close_minus1 - open_0) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToHighEU"] = -round((close_minus1 - highEU_0) / close_minus1, 4)
        df_result.at[i, "_t-1_openToClose"] = -round((open_minus1 - close_minus1) / open_minus1, 4)
        df_result.at[i, "_t-2_closeToClose"] = -round((close_minus2 - close_minus1) / close_minus2, 4)

        df_result.at[i, "dayOfWeek"] = day["date"].isocalendar()[2]




    print("Writing to excel file...")
    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df_result.to_excel(writer, sheet_name='DAX_917_future_script', index=False)

# downloadAnalyticsData()
processAnalyticsData_DAX()