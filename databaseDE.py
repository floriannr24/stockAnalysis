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


def findMarketCap(info):
    try:
        mc = round(info["marketCap"], 2)
    except:
        mc = None
    return mc


def getMovementAfterOpening(date, code):
    ticker = yf.Ticker(code)
    history = ticker.history(start=date, end=date + timedelta(days=1))

    for i, day in history.iterrows():

        open = round(day["Open"], 2)
        lowest = round(day["Low"], 2)
        highest = round(day["High"], 2)
        close = round(day["Close"], 2)

        movement_low = -round((open - lowest) / open, 4)
        movement_high = -round((open - highest) / open, 4)
        movement_close = -round((open - close) / open, 4)

        return movement_low, movement_high, movement_close


def dateAfterDays(date, days):

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

def getPeakOfNextDay(dateOfEC, code):

    nextDay = dateAfterDays(dateOfEC, 1)

    ticker = yf.Ticker(code)

    try:
        history = ticker.history(start=nextDay, end=nextDay + timedelta(days=1))
    except:
        return None

    for i, day in history.iterrows():

        open = round(day["Open"], 2)
        lowest = round(day["Low"], 2)
        highest = round(day["High"], 2)

        if code == "WAF.DE":
            print(nextDay, open, lowest, highest)

        return -round((open - highest) / open, 4), -round((open - lowest) / open, 4)




def updateDatabase():
    for index, row in df.iterrows():

        if not pd.isnull(df.loc[index, "marketCap"]):
            continue

        ticker = yf.Ticker(row["Code"])
        info = ticker.info

        pe = findPE(info)
        marketCap = findMarketCap(info)

        if pe:
            df.at[index, "P/E_May"] = pe
        if marketCap:
            df.at[index, "marketCap"] = marketCap

        print(index, row["Emittent"])

    with pd.ExcelWriter("C:/Users/FSX-P/bafin/Gesamt.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='Code', index=False)

def downloadAnalyticsData():
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="ProfitForecast DE")

    for index, row in df.iterrows():

        if not pd.isnull(df.loc[index, "nextDayLow"]):
            continue

        code = row["Code"]
        dateOfEC = row["Datum"].to_pydatetime()

        try:
            ticker = yf.Ticker(code)
            info = ticker.info
        except:
            print(f"<< Ticker Error >>")
            continue

        # if pd.isnull(df.loc[index, "marketCap"]):
        #     df.at[index, "P/E"] = findTrailingPE(info)
        #     df.at[index, "marketCap"] = findMarketCap(info)

        try:
            movement_low, movement_high, movement_close = getMovementAfterOpening(dateOfEC, code)
            nextDayPeak, nextDayLow = getPeakOfNextDay(dateOfEC, code)
        except:
            continue

        df.at[index, "low"] = movement_low
        df.at[index, "high"] = movement_high
        df.at[index, "close"] = movement_close
        df.at[index, "nextDayPeak"] = nextDayPeak
        df.at[index, "nextDayLow"] = nextDayLow

        print(index, code)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='ProfitForecastWeltDE_script', index=False)

# updateDatabase()
downloadAnalyticsData()