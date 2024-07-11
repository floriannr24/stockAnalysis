import datetime
from datetime import timedelta, date
import yfinance as yf
import pandas as pd

def nextWorkdayAfterDays(date, days):
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


def findTrailingPE(info):
    try:
        pe = round(info["trailingPE"], 2)
    except:
        try:
            pe = round(info["previousClose"] / info["trailingEps"], 2)
            if pe < 0:
                pe = None
        except:
            pe = None

    return pe


def findForwardPE(info):
    try:
        pe = round(info["forwardPE"], 2)
    except:
        pe = None

    return pe


def findMarketCap(info):
    try:
        mc = round(info["marketCap"], 2)
    except:
        mc = None
    return mc


def findSector(info):
    try:
        sector = info["sector"]
    except:
        sector = None
    return sector


def getMovementAfterOpening(date, nextWorkday,  code):

    ticker = yf.Ticker(code)
    history = ticker.history(start=date, end=nextWorkday)

    for i, day in history.iterrows():

        open = round(day["Open"], 2)
        lowest = round(day["Low"], 2)
        highest = round(day["High"], 2)
        close = round(day["Close"], 2)

        if open != 0:

            movement_low = -round((open - lowest) / open, 4)
            movement_high = -round((open - highest) / open, 4)
            movement_close = -round((open - close) / open, 4)

            return movement_low, movement_high, movement_close

        else:
            raise Exception(f"{code}: Division by 0")


def findCountry(info):
    try:
        country = info["country"]
    except:
        country = None
    return country


def main_db(rebuild):
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="Meta")

    for index, row in df.iterrows():

        if not rebuild:
            if not pd.isnull(df.loc[index, "sector"]):
                continue

        ticker = yf.Ticker(row["Code"])
        info = ticker.info

        sector = findSector(info)
        country = findCountry(info)

        df.at[index, "sector"] = str(sector)
        df.at[index, "country"] = str(country)

        print("database: ", index, row["Name"])

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='Meta', index=False)

def getRunUp(days, dateOfEC, code):

    startDate = nextWorkdayAfterDays(dateOfEC, -days)
    dayBeforeEC = nextWorkdayAfterDays(dateOfEC, -1)

    ticker = yf.Ticker(code)
    startData = ticker.history(start=startDate, end=startDate + timedelta(days=1))
    endData = ticker.history(start=dayBeforeEC, end=dayBeforeEC + timedelta(days=1))

    startPrice = None
    endPrice = None

    for i, day in startData.iterrows():
        startPrice = round(day["Open"], 2)

    for i, day in endData.iterrows():
        endPrice = round(day["High"], 2)

    if startPrice and endPrice:
        movement = -round((startPrice - endPrice) / startPrice, 4)
    else:
        movement = None

    return movement

def getMovementBetweenCloseAndOpen(dateOfEC, code):

    dayBeforeEC = nextWorkdayAfterDays(dateOfEC, -1)
    dayAfterEC = nextWorkdayAfterDays(dateOfEC, 1)

    ticker = yf.Ticker(code)
    startData = ticker.history(start=dayBeforeEC, end=dateOfEC)
    endData = ticker.history(start=dateOfEC, end=dayAfterEC)

    for i, day in startData.iterrows():
        close = round(day["Close"], 2)

    for i, day in endData.iterrows():

        open = round(day["Open"], 2) 

    if close and open:
        return -round((close - open) / close, 4)
    else: 
        return None



def main_movement():
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="ProfitForecastWelt")

    for index, row in df.iterrows():

        if not pd.isnull(df.loc[index, "close"]):
            continue

        code = row["Code"]
        dateOfEC = row["Datum"].to_pydatetime()
        timeOfAnnouncement = row["EC vor/nach"]

        try:
            ticker = yf.Ticker(code)
            info = ticker.info
        except:
            print(f"<< Ticker Error >>")
            continue

        if pd.isnull(df.loc[index, "marketCap"]):
            df.at[index, "P/E"] = findTrailingPE(info)
            df.at[index, "marketCap"] = findMarketCap(info)

        try:
            if timeOfAnnouncement == "nach":
                nextWorkday = nextWorkdayAfterDays(dateOfEC, 1)
                nextWorkdayPlusOneDay = nextWorkdayAfterDays(nextWorkday, 1)
                movement_low, movement_high, movement_close = getMovementAfterOpening(nextWorkday, nextWorkdayPlusOneDay, code) 

            if timeOfAnnouncement == "zwischen":
                nextWorkday = nextWorkdayAfterDays(dateOfEC, 1)
                nextWorkdayPlusOneDay = nextWorkdayAfterDays(nextWorkday, 1)
                movement_between = getMovementBetweenCloseAndOpen(dateOfEC, code)
                movement_low, movement_high, movement_close = getMovementAfterOpening(nextWorkday, nextWorkdayPlusOneDay, code)
                
            if timeOfAnnouncement == "vor":
                nextWorkday = nextWorkdayAfterDays(dateOfEC, 1)
                movement_low, movement_high, movement_close = getMovementAfterOpening(dateOfEC, nextWorkday, code)
        except:
            continue

        df.at[index, "movementAfterOpening_low"] = movement_low
        df.at[index, "movementAfterOpening_high"] = movement_high
        df.at[index, "movementBetweenCloseAndOpen"] = movement_between
        df.at[index, "close"] = movement_close

        print("mvmnt: ", index, code)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='ProfitForecastWelt_script', index=False)

def main_runUp():
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="RunUpWelt")

    for index, row in df.iterrows():

        if not pd.isnull(df.loc[index, "marketCap"]):
            continue

        code = row["Code"]
        dateOfEC = row["Datum"].to_pydatetime()

        try:
            ticker = yf.Ticker(code)
            info = ticker.info
        except:
            print(f"<< Ticker Error >>")
            continue

        try:
            shortName = info["shortName"]
        except:
            shortName = None

        if pd.isnull(df.loc[index, "marketCap"]):
            df.at[index, "P/E"] = findTrailingPE(info)
            df.at[index, "marketCap"] = findMarketCap(info)

        df.at[index, "runUp4Days"] = getRunUp(4, dateOfEC, code)
        df.at[index, "Name"] = shortName

        print(index, code, shortName)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='RunUpWelt_script', index=False)


# main_db(rebuild=False)
main_movement()
# main_runUp()
