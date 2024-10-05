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

    nasdaq_future = "NQ=F"
    nasdaq = "^IXIC"
    sp500 = "ES=F"
    dax = "^GDAXI"

    dateToday = dt.date.today()
    dateYesterday = nextWorkdayAfterDays(dateToday, -720)
    dateTomorrow = nextWorkdayAfterDays(dateToday, 1)     

    ticker = yf.Ticker(dax).history()
    print(ticker)

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='DAX_data', index=True)

        
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="NASDAQ_future_data")

    df['Datetime'] = pd.to_datetime(df["Datetime"])

    df_filtered = df[df["Datetime"].dt.day_of_week != 6] # remove sundays
    df_filtered = df_filtered[(df_filtered["Datetime"].dt.hour == 8) | (df_filtered["Datetime"].dt.hour == 21)] # remove all but 8am, 9am and 10pm
    df_filtered = df_filtered[df_filtered["Datetime"] != "2024-03-28 08:00:00"]
    df_filtered = df_filtered.reset_index(drop=True)

    df_analyze = pd.DataFrame()

    for i, hour in df_filtered.iterrows():

        if (hour["Datetime"].hour == 8) or (i+1 == len(df_filtered)):
            continue
        
        closeAt10pm = hour["Close"]
        openAt8am = df_filtered.at[i + 1, "Open"]

        changeOvernight = -round((closeAt10pm - openAt8am) / closeAt10pm, 4)

        df_analyze.at[i, "datetime"] = hour["Datetime"]
        df_analyze.at[i, "datetime_label"] = df_filtered.at[i+1, "Datetime"]
        df_analyze.at[i, "changeOvernight "] = changeOvernight


    df_analyze = df_analyze.reset_index(drop=True)
    print(df_analyze)

    
    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df_analyze.to_excel(writer, sheet_name='NASDAQ_future_script', index=False)
    
def processAnalyticsData_DAX():
        
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="DAX")

    df['Date'] = pd.to_datetime(df["Date"])

    df_analyze = pd.DataFrame()

    for i, day in df.iterrows():

        if (i+2) > len(df):
            continue
        
        open = day["Open"]
        high = day["High"]
        low = day["Low"]
        close = day["Close"]
        openNextDay = df.at[i+1, "Open"]
        maxNextDay = df.at[i+1, "High"]
        closeNextDay = df.at[i+1 , "Close"]
        lowNextDay = df.at[i+1, "Low"]

        openNextDayToHighNextDay = -round((openNextDay - maxNextDay) / openNextDay, 4)
        openNextDayToLowNextDay = -round((openNextDay - lowNextDay) / openNextDay, 4)
        openToClose = -round((open - close) / open, 4)
        closeToOpenNextDay = -round((close - openNextDay) / close, 4)
        closeToMaxNextDay = -round((close - maxNextDay) / close, 4)
        openNextDayToCloseNextDay = -round((openNextDay - closeNextDay) / openNextDay, 4)

        df_analyze.at[i, "date"] = day["Date"]
        df_analyze.at[i, "open"] = open
        df_analyze.at[i, "high"] = high
        df_analyze.at[i, "low"] = low
        df_analyze.at[i, "close"] = close
        df_analyze.at[i, "openToClose"] = openToClose
        df_analyze.at[i, "openNextDayToHighNextDay"] = openNextDayToHighNextDay
        df_analyze.at[i, "openNextDayToCloseNextDay"] = openNextDayToCloseNextDay
        df_analyze.at[i, "openNextDayToLowNextDay"] = openNextDayToLowNextDay
        df_analyze.at[i, "closeToOpenNextDay"] = closeToOpenNextDay
        df_analyze.at[i, "closeToMaxNextDay"] = closeToMaxNextDay
        df_analyze.at[i, "dayOfWeek"] = day["Date"].isocalendar()[2]

    print(df_analyze)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df_analyze.to_excel(writer, sheet_name='DAX_script', index=False)

# downloadAnalyticsData()
# processAnalyticsData_DAX()