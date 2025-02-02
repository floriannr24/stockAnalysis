from datetime import timedelta
import datetime as dt
import yfinance as yf
import pandas as pd

from nasdaq_future import findOpenClose, findMaximumBetween0800_2200, findMaximumBetween0800_1400, \
    findLowBetween0800_2200, findLowBetween0800_1400, findMaximumBetween1000_2200, findMaximumBetween1000_1400, \
    find1000And1400, find0900, find1500, find08xxAnd09xx, findMaximumBetween08xx_0959, findMondayToFridayPerformance, \
    findPositiveStreak, findNegativeStreak, nextWorkdayAfterDays, holidaysUSA


def loadDataframes():
    print("Loading dataframes")

    df_1h = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="SP500_future")
    df_5m = pd.read_excel("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", sheet_name="SP500_future_15m")
    holidays_datetime = [dt.datetime.strptime(date, '%Y-%m-%d') for date in holidaysUSA]

    df_1h['Datetime'] = pd.to_datetime(df_1h["Datetime"])
    df_1h = df_1h[df_1h["Datetime"] != "2024-03-28 08:00:00"]
    df_1h = df_1h[~df_1h['Datetime'].dt.date.isin([holiday.date() for holiday in holidays_datetime])]

    df_1h = df_1h[df_1h["Datetime"].dt.day_of_week != 6]  # remove sundays

    df_5m['Datetime'] = pd.to_datetime(df_5m["Datetime"])
    df_5m = df_5m[df_5m["Datetime"] != "2024-03-28 08:00:00"]
    df_5m = df_5m[~df_5m['Datetime'].dt.date.isin([holiday.date() for holiday in holidays_datetime])]
    df_5m = df_5m[df_5m["Datetime"].dt.day_of_week != 6]  # remove sundays

    return df_1h, df_5m

def downloadSP500_1h():
    print("Downloading '1h' data...")

    nasdaq_future = "ES=F"

    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -300)

    ticker = yf.Ticker(nasdaq_future).history(start=dateStart, end=dateToday, interval="1h")

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')

    print("Writing to excel file...")

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='sp500_future_data_temp', index=True)

def downloadSP500_5m():
    print("Downloading '5m' data...")

    nasdaq_future = "ES=F"

    dateToday = dt.date.today()
    dateStart = nextWorkdayAfterDays(dateToday, -57)

    ticker = yf.Ticker(nasdaq_future).history(start=dateStart, end=dateToday, interval="5m")

    tickerNoTimezone = ticker.copy()
    tickerNoTimezone.index = tickerNoTimezone.index.strftime('%Y-%m-%d %H:%M:%S')

    print("Writing to excel file...")

    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes_src.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        tickerNoTimezone.to_excel(writer, sheet_name='sp500_future_data_15m_temp', index=True)

def processFutureAnalyticsData():
    df_1h, df_5m = loadDataframes()

    df_openClose = findOpenClose(df_1h)
    df_highFull = findMaximumBetween0800_2200(df_1h)
    df_highEU = findMaximumBetween0800_1400(df_1h)
    df_lowFull = findLowBetween0800_2200(df_1h)
    df_lowEU = findLowBetween0800_1400(df_1h)
    df_10To10pmHigh = findMaximumBetween1000_2200(df_1h)
    df_10To2pmHigh = findMaximumBetween1000_1400(df_1h)
    df_10am2pm = find1000And1400(df_1h)
    df_9am = find0900(df_1h)
    df_3pm = find1500(df_1h)
    df_8am9am = find08xxAnd09xx(df_5m, 30, 55)
    df_high9am = findMaximumBetween08xx_0959(df_5m, 35, 55)
    df_monFriPerf = findMondayToFridayPerformance(df_1h)

    # join all dataframes
    print("Joining dataframes")
    df_joined = df_openClose.join(df_highFull)

    df_joined = df_highEU.join(df_joined)
    df_joined = df_joined.join(df_10am2pm)
    df_joined = df_joined.join(df_10To10pmHigh)
    df_joined = df_joined.join(df_10To2pmHigh)
    df_joined = df_joined.join(df_lowEU)
    df_joined = df_joined.join(df_lowFull)
    df_joined = df_joined.join(df_9am)
    df_joined = df_joined.join(df_3pm)
    df_joined = df_joined.join(df_8am9am, how="left")
    df_joined = df_joined.join(df_high9am, how="left")
    df_joined = df_joined.join(df_monFriPerf, how="left")

    df_joined = df_joined.reset_index()

    df_joined = findPositiveStreak(df_joined)
    df_joined = findNegativeStreak(df_joined)

    # final sheet
    print("Final sheet")
    df_result = pd.DataFrame()

    for i, day in df_joined.iterrows():

        if i - 1 < 0 or i - 2 < 0 or i - 3 < 0 or i + 1 > len(df_joined) - 1:
            continue

        # EU
        openEU_0 = day["open"]
        highEU_0 = day["high"]
        lowEU_0 = day["low"]
        val1000 = day["val1000"]
        val1400 = day["val1400"]
        val08xx = day["val08xx"]
        val09xx = day["val09xx"]
        val08xx09xxHigh = day["val08xx09xxHigh"]
        val10amTo10pmHigh = day["val10amTo10pmHigh"]
        val10amTo2pmHigh = day["val10amTo2pmHigh"]
        val0900 = day["val0900"]
        valCustom = day["valCustom"]
        val1500 = day["val1500"]

        openEU_plus1 = df_joined.at[i + 1, "open"]
        val1400_plus1 = df_joined.at[i + 1, "val1400"]
        openEU_minus1 = df_joined.at[i - 1, "open"]
        openEU_minus2 = df_joined.at[i - 2, "open"]
        openEU_minus3 = df_joined.at[i - 3, "open"]

        # US
        high_0 = day["high_full"]
        low_0 = day["low_full"]
        close_0 = day["close"]
        high_time = day["high_time_full"]

        high_minus1 = df_joined.at[i - 1, "high_full"]
        high_plus1 = df_joined.at[i + 1, "high_full"]
        close_plus1 = df_joined.at[i + 1, "close"]
        close_minus1 = df_joined.at[i - 1, "close"]
        close_minus2 = df_joined.at[i - 2, "close"]
        close_minus3 = df_joined.at[i - 3, "close"]

        # special
        _2100open = day["2100"]
        _2100high = day["2100high"]
        _2100low = day["2100low"]
        percentBuffer = 0.0
        _monFriPerf = day["monFriPerf"]
        va08xxHighTime = day["val08xxHigh_time"]

        df_result.at[i, "date"] = day["date"]
        df_result.at[i, "_openToClose"] = -round((openEU_0 - close_0) / openEU_0, 4)
        df_result.at[i, "_openToHighEU"] = -round((openEU_0 - highEU_0) / openEU_0, 4)
        df_result.at[i, "_openToHigh"] = -round((openEU_0 - high_0) / openEU_0, 4)
        df_result.at[i, "_1000To2pm"] = -round((val1000 - val1400) / val1000, 4)
        df_result.at[i, "_1000ToHigh"] = -round((val1000 - val10amTo10pmHigh) / val1000, 4)
        df_result.at[i, "_openTo10am"] = -round((openEU_0 - val1000) / openEU_0, 4)
        df_result.at[i, "_openNextDayToHighNextDay"] = -round((openEU_plus1 - high_plus1) / openEU_plus1, 4)
        df_result.at[i, "_openNextDayToCloseNextDay"] = -round((openEU_plus1 - close_plus1) / openEU_plus1, 4)
        df_result.at[i, "_closeToCloseNextDay"] = -round((close_0 - close_plus1) / close_0, 4)
        df_result.at[i, "_openTo08xx"] = -round((openEU_0 - val08xx) / openEU_0, 4)
        df_result.at[i, "_08xxToHigh"] = -round((val08xx - high_0) / val08xx, 4)
        df_result.at[i, "_08xxToCustom"] = -round((val08xx - val1400) / val08xx, 4)
        df_result.at[i, "_0900ToHigh"] = -round((val0900 - highEU_0) / val0900, 4)
        df_result.at[i, "_openToCustom"] = -round((openEU_0 - val0900) / openEU_0, 4)
        df_result.at[i, "_customToCustom"] = -round((close_minus1 - openEU_0) / close_minus1, 4)
        df_result.at[i, "_customToHigh"] = -round((val0900 - highEU_0) / val0900, 4)
        df_result.at[i, "_openToLowEU"] = -round((openEU_0 - lowEU_0) / openEU_0, 4)
        df_result.at[i, "_openToLow"] = -round((openEU_0 - low_0) / openEU_0, 4)
        df_result.at[i, "_openTo1500"] = -round((openEU_0 - val1500) / openEU_0, 4)

        df_result.at[i, "_t-1_closeTo0900"] = -round((close_minus1 - val0900) / close_minus1, 4)
        df_result.at[i, "_t-1_openToClose"] = -round((openEU_minus1 - close_minus1) / openEU_minus1, 4)
        df_result.at[i, "_t-1_openToHigh"] = -round((openEU_minus1 - high_minus1) / openEU_minus1, 4)
        df_result.at[i, "_t-1_openToClose"] = -round((openEU_minus1 - close_minus1) / openEU_minus1, 4)
        df_result.at[i, "_t-1_closeTo10am"] = -round((close_minus1 - val1000) / close_minus1, 4)
        df_result.at[i, "_t-1_closeTo08xx"] = -round((close_minus1 - val08xx) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToOpen"] = -round((close_minus1 - openEU_0) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToHigh"] = -round((close_minus1 - high_0) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToClose"] = -round((close_minus1 - close_0) / close_minus1, 4)
        df_result.at[i, "_t-1_closeToCustom"] = -round((close_minus1 - valCustom) / close_minus1, 4)
        df_result.at[i, "_t-1_closeTo1500"] = -round((close_minus1 - val1500) / close_minus1, 4)  #
        df_result.at[i, "_t-2_openToClose"] = -round((openEU_minus2 - close_minus2) / openEU_minus2, 4)
        df_result.at[i, "_t-2_closeToClose"] = -round((close_minus2 - close_minus1) / close_minus2, 4)
        df_result.at[i, "_t-2_closeToOpen"] = -round((close_minus2 - openEU_0) / close_minus2, 4)
        df_result.at[i, "_t-3_openToClose"] = -round((openEU_minus3 - close_minus3) / openEU_minus3, 4)

        df_result.at[i, "_closeToOpenNextDay"] = -round((close_0 - openEU_plus1) / close_0, 4)
        df_result.at[i, "_closeToMaxNextDay"] = -round((close_0 - high_plus1) / close_0, 4)
        df_result.at[i, "_nextDayOpenToNextDay1359"] = -round((openEU_plus1 - val1400_plus1) / openEU_plus1, 4)
        df_result.at[i, "_21ToHigh"] = -round((_2100open - _2100high) / _2100open, 4)
        df_result.at[i, "_21ToClose"] = -round((_2100open - close_0) / _2100open, 4)
        df_result.at[i, "_21ToLow"] = -round((_2100open - _2100low) / _2100open, 4)
        df_result.at[i, "_highTo21"] = -round((high_0 - _2100open) / high_0, 4)
        df_result.at[i, "_highToClose"] = -round((high_0 - close_0) / high_0, 4)
        df_result.at[i, "_08xxTo09xx"] = -round((val08xx - val09xx) / val08xx, 4)
        df_result.at[i, "_08xxTo09xxHigh"] = -round((val08xx - val08xx09xxHigh) / val08xx, 4)
        df_result.at[i, "_monFriPerf"] = _monFriPerf

        df_result.at[i, "dayOfWeek"] = day["date"].isocalendar()[2]
        df_result.at[i, "val08xxTo09xxHighTime"] = va08xxHighTime
        df_result.at[i, "val_highTime"] = high_time

        # aus Sicht "heute": wie viele vergangene Tage mit positive/negatvie run?
        df_result.at[i, "ctc_positiveRun"] = df_joined.at[i - 2, "ctc_positiveRun"]
        df_result.at[i, "ctc_negativeRun"] = df_joined.at[i - 2, "ctc_negativeRun"]

        df_result.at[i, "bool_isHighEUTodayDay>=ClosePreviousDay"] = "yes" if highEU_0 >= close_minus1 else "no"
        df_result.at[i, "bool_isHighUSTodayDay>=ClosePreviousDay"] = "yes" if high_0 >= close_minus1 else "no"
        df_result.at[i, "bool_is08xxValReachedBetweem10am10pm"] = "yes" if val10amTo10pmHigh >= val08xx else "no"
        df_result.at[i, "bool_is08xxValReachedBetweem10am2pm"] = "yes" if val10amTo2pmHigh >= val08xx else "no"
        df_result.at[i, "bool_isCloseValReachedBetween10am10pm"] = "yes" if val10amTo10pmHigh >= close_minus1 * ( 1 - percentBuffer * 0.01) else "no"
        df_result.at[i, "bool_isCloseValReachedBetween10am2pm"] = "yes" if val10amTo2pmHigh >= close_minus1 * (1 - percentBuffer * 0.01) else "no"

    print("Writing to Excel")
    with pd.ExcelWriter("C:/Users/FSX-P/Aktienanalyse/Indizes.xlsx", engine='openpyxl', mode='a',
                        if_sheet_exists="replace") as writer:
        df_result.to_excel(writer, sheet_name='SP500_future_script', index=False)


if __name__ == "__main__":
    # downloadSP500_5m()
    # downloadSP500_1h()
    processFutureAnalyticsData()