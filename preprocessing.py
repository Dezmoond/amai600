import asyncio
import pandas as pd
from typing import List, Dict, Tuple
import numpy as np
from sklearn.linear_model import LinearRegression
import logging
from datetime import datetime
from features.logging import setup_logger




async def convert_to_datetime(df: pd.DataFrame,
                              logger: logging.Logger = setup_logger()
                              ) -> pd.DataFrame:
    """
    Конвертирует колонку 'source_time' в datetime.
    """
    def convert_unixtime_to_datetime(timestamp: int) -> pd.Timestamp:
        return datetime.fromtimestamp((timestamp/10000000)-11644473600)

    df['timestamp'] = df['timestamp'].apply(convert_unixtime_to_datetime)
    #drop timestamp column
    #df = df.drop(columns=['timestamp'])
    return df


async def drop_negative_values(df: pd.DataFrame,
                              logger: logging.Logger = setup_logger()
                              ) -> pd.DataFrame:
    """
    Удаляет отрицательные значения из DataFrame.
    """
    df = df[df['value'] >= 0]
    return df


async def get_pivot_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    переводим  archive_itemid  в колонки, заполняем пропуски ffill

    :param df: исходный DataFrame с колонками 'source_time', 'id', 'value'
    :return: pivot-таблица с timestamp в индексе и id в колонках
    """
    df = df.pivot_table(index='timestamp', columns='archive_itemid', values='value', aggfunc='mean')
    df.columns.name = None
    df.ffill(inplace=True)
    return df

async def check_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Проверяет наличие всех необходимых колонок в DataFrame.
    """
    required_columns = [34, 565, 566, 567, 568, 569, 570, 571, 600, 601, 602, 603, 604]
    
    #add missing columns with value nan
    for column in required_columns:
        if column not in df.columns:
            df[column] = float('nan')
    return df

async def aggregate_data(df: pd.DataFrame, aggregate_time: str = '1s') -> pd.DataFrame:
    """
    Агрегирует данные по 1 секундам кроме колонок 600, 601, 602
    """
    df = df.resample(aggregate_time).mean()
    return df


async def get_finaly_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Получает финальный DataFrame для модели.
    """
    df = await convert_to_datetime(df)
    df = await drop_negative_values(df)

    df = await get_pivot_df(df)
    df = await check_columns(df)
    df = await aggregate_data(df)
    return df


async def calculate_features(df: pd.DataFrame, 
                             sensor_cols: List[str] = [34, 565, 566, 567, 568, 569, 570, 571, 603, 604],
                             logger: logging.Logger = setup_logger()
                             ) -> Tuple[List[Dict[str, float]], List[int]]:
    feature_dicts = []

    df = df.sort_values(by='timestamp')
    df = df.reset_index()
    #drop 600, 601, 602 columns
    df = df.drop(columns=[600, 601, 602])

    df['relative_time'] = df['timestamp'].transform(lambda x: (x.max() - x).dt.total_seconds())
    sensor_cols = [c for c in df.columns if isinstance(c, int)]
    times = df['relative_time'].values.reshape(-1, 1)
    window = df[sensor_cols].interpolate().fillna(method='ffill').fillna(0)


    features = {}
    for s in sensor_cols:
        vals = window[s].values
        features[f'{s}_mean']  = np.mean(vals)
        features[f'{s}_std']   = np.std(vals)
        features[f'{s}_min']   = np.min(vals)
        features[f'{s}_max']   = np.max(vals)
        features[f'{s}_last']  = vals[-1]
        if len(vals) > 1:
            lr = LinearRegression().fit(times, vals)
            features[f'{s}_slope'] = lr.coef_[0]
        else:
            features[f'{s}_slope'] = 0.0

    feature_dicts.append(features)
    X = pd.DataFrame(feature_dicts)
    return X


async def main_preprocessing(df: pd.DataFrame,
              logger: logging.Logger = setup_logger()
              ) -> pd.DataFrame:
    

    df = await get_finaly_df(df)
    features = await calculate_features(df)
    return features


#df = pd.read_excel(r'D:\Projects\AMAI\Eugene_Sapsalev\app\test_data_rt.xlsx')
#df = asyncio.run(main(df))

#res = asyncio.run(work_with_model.main(df))
#print(res)