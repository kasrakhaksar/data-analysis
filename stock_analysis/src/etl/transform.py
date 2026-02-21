import pandas as pd
from datetime import datetime

def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df['insCode'] = '35425587644337450'
    df['dEven'] = "20260218"


    df['Time_2'] = df['hEven'].astype(str).apply(lambda x: ':'.join([x[:2], x[2:4], x[4:]]) if len(x) == 6 else '0' + ':'.join([x[:1], x[1:3], x[3:]]))

    df['Time_2'] = df['Time_2'].apply(lambda x: str(x).split(':')[0]+":"+str(x).split(':')[1])
    df['Time_2'] = df['Time_2'].apply(lambda x: ':'.join([i.zfill(2) if len(i) == 1 else i for i in x.split(':')]))


    df['jdate'] = df['dEven'].apply(lambda x: x[:4]+"-"+x[4:6]+"-"+ x[6:])
    df = df.drop(['iClose','yClose'], axis=1)


    df = df[(df["qTotTran5J"] != 0.0) | (df["qTotTran5J"] != 0)]


    all_df = []

    df2 = df.groupby(['insCode','jdate', 'Time_2'])['pDrCotVal'].last().reset_index()
    df2.rename(columns={'pDrCotVal': 'open'}, inplace=True)

    df3 = df.groupby(['insCode','jdate', 'Time_2'])['pDrCotVal'].first().reset_index()
    df3.rename(columns={'pDrCotVal': 'close'}, inplace=True)

    df4 = df.groupby(['insCode','jdate', 'Time_2'])['pDrCotVal'].max().reset_index()
    df4.rename(columns={'pDrCotVal': 'high'}, inplace=True)

    df5 = df.groupby(['insCode','jdate', 'Time_2'])['pDrCotVal'].min().reset_index()
    df5.rename(columns={'pDrCotVal': 'low'}, inplace=True)

    df6 = df.groupby(['insCode','jdate', 'Time_2'])['qTotTran5J'].sum().reset_index()
    df6.rename(columns={'qTotTran5J': 'volume'}, inplace=True)


    savedf = df2.merge(df3, on=['insCode','jdate', 'Time_2'])
    savedf = savedf.merge(df4, on=['insCode','jdate', 'Time_2'])
    savedf = savedf.merge(df5, on=['insCode','jdate', 'Time_2'])
    savedf = savedf.merge(df6, on=['insCode','jdate', 'Time_2'])


    savedf['Time_2'] = savedf['Time_2'].astype(str)
    savedf['jdate'] = savedf['jdate'].astype(str)
    savedf['jdateTime'] = savedf['jdate'] + ' ' + savedf['Time_2']
    
    savedf = savedf.drop(['Time_2'], axis=1)
    savedf = savedf.rename(columns={'jdateTime': 'jdates'})
    savedf = savedf.drop(['jdate'], axis=1)


    df['hEven'] = df['hEven'].astype(str)
    df['hEven'] = df['hEven'].astype(str).apply(lambda x: ':'.join([x[:2], x[2:4], x[4:]]) if len(x) == 6 else '0' + ':'.join([x[:1], x[1:3], x[3:]]))
    df['jdate'] = df['jdate'].astype(str)
    df['jdateTime'] = df['jdate'] + ' ' + df['hEven']
    df = df.drop(['jdate', 'id' , 'Time_2'], axis=1)
    df = df.rename(columns={'jdateTime': 'jdate'})
    df['hEven'] = df['hEven'].str.replace(':','')
    df['dEven'] = df['dEven'].astype(int)


    savedf['jdates'] = savedf['jdates'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M"))
    
    all_df.append(savedf)
    final_df = pd.concat(all_df)
    
    return final_df
