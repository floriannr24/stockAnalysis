import datetime
from datetime import timedelta, date
import yfinance as yf
import pandas as pd

THR = -0.02


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


def findSector(info):
    try:
        sector = info["sector"]
    except:
        sector = None
    return sector


def main_db():
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="Meta")

    for index, row in df.iterrows():

        if not pd.isnull(df.loc[index, "marketCap"]):
            continue

        ticker = yf.Ticker(row["Code"])
        info = ticker.info

        pe = findPE(info)
        marketCap = findMarketCap(info)
        sector = findSector(info)

        if pe:
            df.at[index, "P/E_May"] = pe
        if marketCap:
            df.at[index, "marketCap"] = marketCap
        if sector:
            df.at[index, "sector"] = str(sector)

        print(index, row["Name"], sector)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='Meta', index=False)


def getMovementAfterOpening(date, code):
    ticker = yf.Ticker(code)
    history = ticker.history(start=date, end=date + timedelta(days=1))

    for i, day in history.iterrows():

        open = round(day["Open"], 2)
        lowest = round(day["Low"], 2)
        highest = round(day["High"], 2)
        movement = round((lowest / open) - 1, 4)

        text = None

        if movement <= THR:
            text = "runter"

        if THR < movement:
            movement = round((highest / open) - 1, 4)
            if movement < -THR:
                text = "keine Verä."
            text = "hoch"

        return movement, text


def main2():
    df = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", sheet_name="ProfitForecastWelt")

    df["movementAfterOpening"] = 0

    for index, row in df.iterrows():

        code = row["Code"]
        dateOfEC = row["Datum"].to_pydatetime()
        afterOrBeforeOpen = row["EC vor/nach"]

        if pd.isnull(afterOrBeforeOpen):
            movement, text = getMovementAfterOpening(dateOfEC, code)
        else:
            movement, text = getMovementAfterOpening(dateOfEC + timedelta(days=+1), code)

        df.at[index, "movementAfterOpening"] = movement
        df.at[index, "Richtung nach Börsenöffnung"] = text

        print(index, code, movement)

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Aktien.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='ProfitForecastWelt_script', index=False)


main2()
