import datetime
from datetime import timedelta, date
import yfinance as yf
import pandas as pd

df = pd.read_excel("C:/Users/FSX-P/bafin/Gesamt.xlsx", sheet_name="Code")

upperBorder = 0.02
lowerBorder = 0.08


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
