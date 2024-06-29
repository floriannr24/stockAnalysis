import datetime
from datetime import timedelta, date
import yfinance as yf
import pandas as pd

df = pd.read_excel("C:/Users/FSX-P/bafin/Gesamt.xlsx", sheet_name="Code")

def findPE(info):
    try:
        pe = round(info["trailingPE"], 2)
    except:
        try:
            pe = round(abs(info["previousClose"] / info["trailingEps"]), 2)
        except:
            pe = None

    return pe

def findMarketCap(code):

    ticker = yf.Ticker(code)
    info = ticker.info

    try:
        mc = round(info["marketCap"], 2)
    except:
        mc = None
    return mc

def getMovementToday(date, code):

    nextWorkday = nextWorkdayAfterDays(date, 1)

    ticker = yf.Ticker(code)
    history = ticker.history(start=date, end=nextWorkday)

    movement_low = None
    movement_high = None
    movement_close = None

    for i, day in history.iterrows():

        open = round(day["Open"], 2)
        lowest = round(day["Low"], 2)
        highest = round(day["High"], 2)
        close = round(day["Close"], 2)

        movement_low = -round((open - lowest) / open, 4)
        movement_high = -round((open - highest) / open, 4)
        movement_close = -round((open - close) / open, 4)

    return movement_low, movement_high, movement_close

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

def getMovementNextDay(dateOfEC, code):

    nextWorkday = nextWorkdayAfterDays(dateOfEC, 1)
    nextWorkdayPlusOneDay = nextWorkdayAfterDays(nextWorkday, 1)

    ticker = yf.Ticker(code)

    try:
        history_nextDay = ticker.history(start=nextWorkday, end=nextWorkdayPlusOneDay)
    except:
        return None
    
    percentage_nextDay_high = None
    percentage_nextDay_low = None
    percentage_nextDay_close = None
    
    for i, day in history_nextDay.iterrows():

        open_nextDay = round(day["Open"], 2)
        lowest_nextDay = round(day["Low"], 2)
        highest_nextDay = round(day["High"], 2)
        close_nextDay = round(day["Close"], 2)

    # percentage diff from open_nextDay
    # if 0: unplausible data
    if open_nextDay != 0:
        percentage_nextDay_high = -round((open_nextDay - highest_nextDay) / open_nextDay, 4)
        percentage_nextDay_low = -round((open_nextDay - lowest_nextDay) / open_nextDay, 4)
        percentage_nextDay_close = -round((open_nextDay - close_nextDay) / open_nextDay, 4)

    return percentage_nextDay_high, percentage_nextDay_low, percentage_nextDay_close

def getWeekOfYear(dateOfEC):
    return dateOfEC.isocalendar()[1]

def getMovementNextDay_special(dateOfEC, code):
    nextWorkday = nextWorkdayAfterDays(dateOfEC, 1)
    nextWorkdayPlusOneDay = nextWorkdayAfterDays(nextWorkday, 1)

    ticker = yf.Ticker(code)

    try:
        history_nextDay = ticker.history(start=nextWorkday, end=nextWorkdayPlusOneDay)
        history_today = ticker.history(start=dateOfEC, end=nextWorkday)
    except:
        return None
    
    close_today = 0
    open_nextDay = 0
    lowest_nextDay = 0
    mvtCloseTodayToLowestNextDay = None
    mvtCloseTodayToOpenNextDay = None
    
    for i, day in history_today.iterrows():
        close_today = round(day["Close"], 2)

    for i, day in history_nextDay.iterrows():

        open_nextDay = round(day["Open"], 2)
        lowest_nextDay = round(day["Low"], 2)

    if close_today != 0:
        mvtCloseTodayToLowestNextDay = -round((close_today - lowest_nextDay) / close_today, 4)
        mvtCloseTodayToOpenNextDay = -round((close_today - open_nextDay) / close_today, 4)

    return mvtCloseTodayToLowestNextDay, mvtCloseTodayToOpenNextDay

def downloadAnalyticsData():
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="ProfitForecast DE")

    for index, row in df.iterrows():

        if not pd.isnull(df.loc[index, "weekOfYear"]):
            continue

        code = row["Code"]
        dateOfEC = row["Datum"].to_pydatetime()

        try:
            low, high, close = getMovementToday(dateOfEC, code)
            high_nextDay, low_nextDay, close_nextDay = getMovementNextDay(dateOfEC, code)
            closeTodayLowestNextDay, closeTodayOpenNextDay = getMovementNextDay_special(dateOfEC, code)
            weekOfYear = getWeekOfYear(dateOfEC)
            marketCap =  findMarketCap(code)
        except:
            continue

        df.at[index, "low"] = low
        df.at[index, "high"] = high
        df.at[index, "close"] = close
        df.at[index, "high_nextDay"] = high_nextDay
        df.at[index, "low_nextDay"] = low_nextDay
        df.at[index, "close_nextDay"] = close_nextDay
        df.at[index, "closeToLowNextDay"] = closeTodayLowestNextDay
        df.at[index, "closeToOpenNextDay"] = closeTodayOpenNextDay
        df.at[index, "weekOfYear"] = weekOfYear
        df.at[index, "marketCap"] = marketCap

        print(index, code)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='ProfitForecastWeltDE_script', index=False)


downloadAnalyticsData()