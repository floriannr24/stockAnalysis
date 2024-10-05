from datetime import timedelta
import yfinance as yf
import pandas as pd

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
        low = round(day["Low"], 2)
        high = round(day["High"], 2)
        close = round(day["Close"], 2)

    if dataIsValid(open, low, high, close):
        movement_low = -round((open - low) / open, 4)
        movement_high = -round((open - high) / open, 4)
        movement_close = -round((open - close) / open, 4)
    else: 
        return None
    
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
        low_nextDay = round(day["Low"], 2)
        high_nextDay = round(day["High"], 2)
        close_nextDay = round(day["Close"], 2)

    if dataIsValid(open_nextDay, low_nextDay, high_nextDay, close_nextDay):
        percentage_nextDay_high = -round((open_nextDay - high_nextDay) / open_nextDay, 4)
        percentage_nextDay_low = -round((open_nextDay - low_nextDay) / open_nextDay, 4)
        percentage_nextDay_close = -round((open_nextDay - close_nextDay) / open_nextDay, 4)
    else: 
        return None

    return percentage_nextDay_high, percentage_nextDay_low, percentage_nextDay_close

def dataIsValid(*args):
    return not all(value == args[0] for value in args)

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
    low_nextDay = 0
    mvtCloseTodayToLowestNextDay = None
    mvtCloseTodayToOpenNextDay = None
    mvtCloseTodayToCloseNextDay = None
    
    for i, day in history_today.iterrows():
        close_today = round(day["Close"], 2)

    for i, day in history_nextDay.iterrows():

        open_nextDay = round(day["Open"], 2)
        close_nextDay = round(day["Close"], 2)
        low_nextDay = round(day["Low"], 2)

    if dataIsValid(close_today, low_nextDay, open_nextDay):
        mvtCloseTodayToLowestNextDay = -round((close_today - low_nextDay) / close_today, 4)
        mvtCloseTodayToOpenNextDay = -round((close_today - open_nextDay) / close_today, 4)
        mvtCloseTodayToCloseNextDay = -round((close_today - close_nextDay) / close_today, 4)
    else: 
        return None

    return mvtCloseTodayToLowestNextDay, mvtCloseTodayToOpenNextDay, mvtCloseTodayToCloseNextDay

def getYesterdayCloseToTodayOpen(dateOfEC, code):

    yesterday = nextWorkdayAfterDays(dateOfEC, -1)
    nextWorkday = nextWorkdayAfterDays(dateOfEC, 1)

    ticker = yf.Ticker(code)

    try:
        history_yesterday = ticker.history(start=yesterday, end=dateOfEC)
    except:
        return None

    try:
        history_today = ticker.history(start=dateOfEC, end=nextWorkday)
    except:
        return None
        
    for i, day in history_yesterday.iterrows():

        close_yesterday = round(day["Close"], 2)

    for i, day in history_today.iterrows():
        
        open_today = round(day["Open"], 2)

    if dataIsValid(open_today, close_yesterday):
        percentage_yesterdayCloseToTodayOpen = -round((close_yesterday - open_today) / close_yesterday, 4)
    else: 
        return None

    return percentage_yesterdayCloseToTodayOpen


def getDayOfWeek(dateOfEC):
    return dateOfEC.isocalendar()[2]
    

def downloadAnalyticsData():
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="ProfitForecast DE")

    for index, row in df.iterrows():

        if not pd.isnull(df.loc[index, "weekOfYear"]):
            continue

        code = row["Code"]
        dateOfEC = row["Datum"].to_pydatetime()
        df.at[index, "weekOfYear"] = getWeekOfYear(dateOfEC)
        df.at[index, "dayOfWeek"] =  getDayOfWeek(dateOfEC)
        time = row["Zeitpunkt"]
        print(index, code)

        if time == "schluss":
            dateOfEC = nextWorkdayAfterDays(dateOfEC, 1)

        try:
            low, high, close = getMovementToday(dateOfEC, code)
            yesterdayCloseToTodayOpen = getYesterdayCloseToTodayOpen(dateOfEC, code)
            high_nextDay, low_nextDay, close_nextDay = getMovementNextDay(dateOfEC, code)
            closeTodayLowestNextDay, closeTodayOpenNextDay, closeTodayCloseNextDay = getMovementNextDay_special(dateOfEC, code)
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
        df.at[index, "closeToCloseNextDay"] = closeTodayCloseNextDay
        df.at[index, "yesterdayCloseToTodayOpen"] = yesterdayCloseToTodayOpen
    
        if pd.isnull(df.loc[index, "marketCap"]):
            df.at[index, "marketCap"] = findMarketCap(code)



    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='ProfitForecastWeltDE_script', index=False)


downloadAnalyticsData()
