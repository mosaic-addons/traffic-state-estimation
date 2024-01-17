import os.path
import sqlite3
import time
import xml.etree.ElementTree as Et
from typing import Callable

import numpy as np
import pandas as pd

# === CONSTANTS ===
# unit conversion
SECOND_TO_NANO_SECOND = 1000000000
HOUR_TO_SECOND = 60 * 60
MPS_TO_KPH = 60 * 60 / 1000
DAY_AS_DAYTIME = pd.to_datetime(86400 * SECOND_TO_NANO_SECOND)
# sql related
TABLE_TRAVERSAL_MEAN_SPEEDS = 'traversal_metrics'
FCD_RECORDS_TABLE = 'fcd_records'
SQL_TRAVERSAL_MEAN_SPEED = f'SELECT * FROM {TABLE_TRAVERSAL_MEAN_SPEEDS} ORDER BY timeStamp;'
SQL_FCD_RECORDS = f'SELECT * FROM {FCD_RECORDS_TABLE} ORDER BY timeStamp;'
# post-processing related
DEFAULT_TIME_STAMP_COLUMN = 'timeStamp'
TRAVERSAL_SPEEDS_GROUPER = {'temporalMeanSpeed': 'mean', 'spatialMeanSpeed': 'mean', 'samples': 'sum'}
EDGE_DATA_GROUPER = {'speed': 'mean'}


# === File Reading ===
def read_database_data(database_path: str, sql: str, pickle_path: str = None) -> pd.DataFrame:
    """Reads data from a database created by TSE apps and returns a pandas dataframe.
    Data can be stored in a pickle to reduce reading time when executing again. Note: if the source changed the pickle needs to be deleted manually.

    Parameters
    ----------
    database_path: str
        Path to the FcdData SQLite database
    sql: str
        SQL query to be executed for reading
    pickle_path: str
        The path where a pickled version should be stored can help with faster reading times when executed again

    Returns
    -------
    A pandas data frame containing relevant data extracted from an FCD database
    """

    start = time.time()
    if pickle_path is not None and os.path.isfile(pickle_path):
        print('start reading:', pickle_path)
        tmp_speed_data_df = pd.read_pickle(pickle_path)
        print('finish reading:', pickle_path, 'took {time:.2f}s'.format(time=time.time() - start))
    else:
        print('start processing database:', database_path)
        # reading and preprocessing queue data from FCD database
        database_connection = sqlite3.connect(database_path)
        # read sql
        tmp_speed_data_df = pd.read_sql_query(sql, database_connection)
        # create time index
        tmp_speed_data_df.insert(0, 'time', pd.to_datetime(tmp_speed_data_df[DEFAULT_TIME_STAMP_COLUMN]))
        tmp_speed_data_df = tmp_speed_data_df.set_index(['time'])
        if pickle_path is not None:
            tmp_speed_data_df.to_pickle(pickle_path)
        print('finish processing database:', database_path, 'took {time:.2f}s'.format(time=time.time() - start))
    return tmp_speed_data_df


def _yield_edge_element(edge_data_root: Et.Element) -> dict:
    """Returns generator to iteratively return an edge data sample.

    Parameters
    ----------
    edge_data_root: ET.element
        root element of the ElementTree of a SUMO edge data file
    Returns
    -------
    The next entry in the edge data file as a dictionary
    """
    for interval in edge_data_root:
        interval_attr = interval.attrib
        for sample in interval:
            sample_dict = interval_attr.copy()
            sample_dict.update(sample.attrib)
            yield sample_dict


def read_sumo_edge_data(file_path: str, pickle_path: str = None) -> pd.DataFrame:
    """Reads data from a SUMO edge data xml file and returns them in a pandas dataframe removing unnecessary columns.
    Data can be stored in a pickle to reduce reading time when executing again. Note: if the source changed the pickle needs to be deleted manually.
    Parameters
    ----------
    file_path: str
        The path to the SUMO edge file (XML)
    pickle_path: str
        The path where a pickled version should be stored can help with faster reading times when executed again

    Returns
    -------
    A pandas data frame containing relevant data extracted from a SUMO edge data file
    """
    if pickle_path is not None and os.path.isfile(pickle_path):
        tmp_df = pd.read_pickle(pickle_path)
    else:
        # read xml and write to dataframe
        edge_data_xml_tree = Et.parse(file_path)
        edge_data_root = edge_data_xml_tree.getroot()
        tmp_df = pd.DataFrame(list(_yield_edge_element(edge_data_root)))
        # drop unnecessary columns
        tmp_df = tmp_df.drop(
            columns=['end', 'sampledSeconds', 'traveltime', 'waitingTime', 'timeLoss', 'departed', 'arrived', 'entered', 'left', 'laneChangedFrom', 'laneChangedTo', 'teleported'], errors='ignore')
        # fix types
        tmp_df[['begin', 'speed']] = tmp_df[['begin', 'speed']].apply(pd.to_numeric)
        # rename id columns
        tmp_df = tmp_df.rename(columns={'begin': DEFAULT_TIME_STAMP_COLUMN, 'id': 'connectionID'})
        # adjust timeStamp column
        tmp_df[DEFAULT_TIME_STAMP_COLUMN] = tmp_df[DEFAULT_TIME_STAMP_COLUMN].astype(dtype='int64') * SECOND_TO_NANO_SECOND
        # create time index
        tmp_df.insert(0, 'time', pd.to_datetime(tmp_df[DEFAULT_TIME_STAMP_COLUMN]))
        tmp_df = tmp_df.set_index(['time'])
        if pickle_path is not None:
            tmp_df.to_pickle(pickle_path)
    return tmp_df


def _yield_loop_data_element(loop_root: Et.Element):
    """Returns generator to iteratively return a loop data sample.

    Parameters
    ----------
    loop_root: ET.element
        root element of the ElementTree of a SUMO loop data file
    Returns
    -------
    The next entry in the loop data file as a dictionary
    """
    for interval in loop_root:
        sample_dict = interval.attrib
        yield sample_dict


def read_sumo_loop_data(file_path: str, pickle_path: str = None) -> pd.DataFrame:
    """Reads data from a SUMO loop data xml file and returns them in a pandas dataframe removing unnecessary columns.
    Data can be stored in a pickle to reduce reading time when executing again. Note: if the source changed the pickle needs to be deleted manually.
    Parameters
    ----------
    file_path: str
        The path to the SUMO loop file (XML)
    pickle_path: str
        The path where a pickled version should be stored can help with faster reading times when executed again

    Returns
    -------
    A pandas data frame containing relevant data extracted from a SUMO loop data file
    """
    if pickle_path is not None and os.path.isfile(pickle_path):
        tmp_df = pd.read_pickle(pickle_path)
    else:
        loop_data_xml_tree = Et.parse(file_path)
        loop_data_root = loop_data_xml_tree.getroot()
        tmp_df = pd.DataFrame.from_records(_yield_loop_data_element(loop_data_root))
        # drop unnecessary columns
        tmp_df = tmp_df.drop(columns=['length'])
        # fix types
        tmp_df[['begin', 'end', 'nVehContrib', 'nVehEntered', 'flow', 'occupancy', 'speed', 'harmonicMeanSpeed']] = tmp_df[
            ['begin', 'end', 'nVehContrib', 'nVehEntered', 'flow', 'occupancy', 'speed', 'harmonicMeanSpeed']].apply(pd.to_numeric)
        # create time index
        tmp_df.insert(0, 'time', pd.to_datetime(tmp_df.begin * SECOND_TO_NANO_SECOND))
        tmp_df = tmp_df.set_index(['time'])
        if pickle_path is not None:
            tmp_df.to_pickle(pickle_path)
    return tmp_df


# === Preprocessing ===
def resample_speed_data(dataframe: pd.DataFrame, grouper: dict[str, str], window: str = '15Min', time_frame: (float, float) = (0, 24),
                        reindex_interval: bool = False, fill_method: str = 'ffill', rolling_window: str = None) -> pd.DataFrame:
    """
    Resamples the FCD speed data for a given dataframe which should only contain the timeseries data for one edge.
    This will also try to add a `samples` column to the dataframe identifying how many samples were used to generate an entry.
    Note: calling this on the entire dataframe will aggregate values over all edges.

    Parameters
    ----------
    dataframe: pd.DataFrame
        a dataframe containing the read database table from an FCD Database
    grouper: dict[str, str]
        a dictionary defining the aggregation methods for different rows in the dataframe
    time_frame: (float, float)
        A tuple of defining the time frame in which the returned data should be to get data between 6am and 6pm use (6, 18)
    window: str
        a time string defining the aggregation window (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    reindex_interval: bool
        A flag defining whether the dataframe index should be reindexed to be *continuous* adding samples to each second
    fill_method: str, {{'ffill', 'bfill'}}
        requires `reindex_interval` to be `true`. Method to be used when reindexing the dataframe
    rolling_window: bool
        A flag that enables rolling window aggregation
    Returns
    -------
        Dataframe
            A resampled dataframe containing aggregated values for the given time window.
    """
    tmp_df = dataframe.copy()
    new_grouper = grouper.copy()
    # count samples by using sample amount column or manually counting all rows within a timeinterval
    if 'sampleAmount' not in tmp_df.columns:
        tmp_df['samples'] = tmp_df.groupby(pd.Grouper(freq=window)).transform('count').iloc[:, 0]
        new_grouper['samples'] = 'mean'
    # do the actual resampling
    tmp_df = tmp_df.groupby(pd.Grouper(freq=window)).agg(new_grouper)
    # if samples were added make sure missing values are fitted with 0 and the type is correct
    if 'sampleAmount' not in tmp_df.columns:
        tmp_df['samples'] = tmp_df['samples'].fillna(value=0)
        tmp_df['samples'] = pd.to_numeric(tmp_df['samples']).astype(int)
    return _finalize_resampling(tmp_df, window=window, reindex_interval=reindex_interval, fill_method=fill_method, time_frame=time_frame, rolling_window=rolling_window)


def resample_edge_data(dataframe: pd.DataFrame, grouper: dict[str, str], window: str = '15Min', fill_method: str = 'ffill', time_frame: (float, float) = (0, 24),
                       reindex_interval: bool = False, rolling_window: str = None) -> pd.DataFrame:
    """
    Resamples the SUMO edge data for a given dataframe which should only contain the timeseries data for one edge.
    Note: calling this on the entire dataframe will aggregate values over all edges.

    Parameters
    ----------
    dataframe: pd.DataFrame
        a dataframe containing the read database table from a SUMO edge data xml
    grouper: dict[str, str]
            a dictionary defining the aggregation methods for different rows in the dataframe
    time_frame: (float, float)
        A tuple of defining the time frame in which the returned data should be to get data between 6am and 6pm use (6, 18)
    window: str
        a time string defining the aggregation window (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    reindex_interval: bool
        A flag defining whether the dataframe index should be reindexed to be *continuous* adding samples to each second
    fill_method: str, {{'ffill', 'bfill'}}
        requires `reindex_interval` to be `true`. Method to be used when reindexing the dataframe
    rolling_window: bool
        A flag that enables rolling window aggregation
    Returns
    -------
        Dataframe
            A resampled dataframe containing aggregated values for the given time window.
    """
    tmp_df = dataframe.copy()
    tmp_df = tmp_df.groupby(pd.Grouper(freq=window)).agg(grouper)
    return _finalize_resampling(tmp_df, reindex_interval=reindex_interval, fill_method=fill_method, time_frame=time_frame, rolling_window=rolling_window)


def resample_traversal_data_per_edge(dataframe: pd.DataFrame, window: str = '15Min', reindex_interval: bool = False, fill_method: str = 'ffill',
                                     time_frame: (float, float) = (0, 24), rolling_window: str = None) -> pd.DataFrame:
    """
    This method uses :func:`_resample_speed_data_per_edge` to create a multi-indexed dataframe using the time and the edge id as index columns looking at each edge independently.

    Parameters
    ----------
    dataframe: pd.DataFrame
        a dataframe containing the read database table from an FCD Database

    time_frame: (float, float)
        A tuple of defining the time frame in which the returned data should be to get data between 6am and 6pm use (6, 18)
    window: str
        a time string defining the aggregation window (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    reindex_interval: bool
        A flag defining whether the dataframe index should be reindexed to be *continuous* adding samples to each second
    fill_method: str, {{'ffill', 'bfill'}}
        requires `reindex_interval` to be `true`. Method to be used when reindexing the dataframe
    rolling_window: bool
        A flag that enables rolling window aggregation
    Returns
    -------
        Dataframe
            A multi-indexed resampled dataframe containing aggregated values for the given time window for all edges defined within the given dataframe.
    """
    return _resample_data_per_edge(resample_speed_data, dataframe, grouper=TRAVERSAL_SPEEDS_GROUPER, window=window, time_frame=time_frame, reindex_interval=reindex_interval, fill_method=fill_method,
                                   rolling_window=rolling_window)


def resample_edge_data_per_edge(dataframe: pd.DataFrame, window: str = '15Min', time_frame: (int, int) = (0, 24), reindex_interval: bool = False, fill_method: str = 'ffill',
                                rolling_window: str = None) -> pd.DataFrame:
    """
    This method uses :func:`_resample_speed_data_per_edge` to create a multi-indexed dataframe using the time and the edge id as index columns looking at each edge independently.

    Parameters
    ----------
    dataframe: pd.DataFrame
        a dataframe containing the read database table from an FCD Database
    time_frame: (float, float)
        A tuple of defining the time frame in which the returned data should be to get data between 6am and 6pm use (6, 18)
    window: str
        a time string defining the aggregation window (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    reindex_interval: bool
        A flag defining whether the dataframe index should be reindexed to be *continuous* adding samples to each second
    fill_method: str, {{'ffill', 'bfill'}}
        requires `reindex_interval` to be `true`. Method to be used when reindexing the dataframe
    rolling_window: bool
        A flag that enables rolling window aggregation
    Returns
    -------
        Dataframe
            A multi-indexed resampled dataframe containing aggregated values for the given time window for all edges defined within the given dataframe.
    """
    return _resample_data_per_edge(resample_speed_data, dataframe, grouper=EDGE_DATA_GROUPER, window=window, time_frame=time_frame, reindex_interval=reindex_interval, fill_method=fill_method,
                                   rolling_window=rolling_window)


def resample_custom_data_per_edge(dataframe: pd.DataFrame, grouper: dict[str, str], window: str = '15Min', reindex_interval: bool = False, fill_method: str = 'ffill',
                                  time_frame: (float, float) = (0, 24), rolling_window: str = None) -> pd.DataFrame:
    """
    This method uses :func:`_resample_speed_data_per_edge` to create a multi-indexed dataframe using the time and the edge id as index columns looking at each edge independently.

    Parameters
    ----------
    dataframe: pd.DataFrame
        a dataframe containing the read database table from an FCD Database
    grouper: dict[str, str]
        a dictionary defining the aggregation methods for different rows in the dataframe
    time_frame: (float, float)
        A tuple of defining the time frame in which the returned data should be to get data between 6am and 6pm use (6, 18)
    window: str
        a time string defining the aggregation window (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    reindex_interval: bool
        A flag defining whether the dataframe index should be reindexed to be *continuous* adding samples to each second
    fill_method: str, {{'ffill', 'bfill'}}
        requires `reindex_interval` to be `true`. Method to be used when reindexing the dataframe
    rolling_window: bool
        A flag that enables rolling window aggregation
    Returns
    -------
        Dataframe
            A multi-indexed resampled dataframe containing aggregated values for the given time window for all edges defined within the given dataframe.
    """
    return _resample_data_per_edge(resample_speed_data, dataframe, grouper=grouper, window=window, time_frame=time_frame, reindex_interval=reindex_interval, fill_method=fill_method,
                                   rolling_window=rolling_window)


def _finalize_resampling(dataframe: pd.DataFrame, window: str = '15min', reindex_interval: bool = False, fill_method: str = 'ffill', time_frame: (float, float) = (0, 24),
                         rolling_window: str = None) -> pd.DataFrame:
    """
    Helper function for :func:resample_speed_data and :func:resample_edge_data applying configured alterations to the resampled dataframes.

    Parameters
    ----------
    dataframe: pd.DataFrame
        a previously resampled dataframe
    time_frame: (float, float)
        A tuple of defining the time frame in which the returned data should be to get data between 6am and 6pm use (6, 18)
    window: str
        a time string defining the aggregation window (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    reindex_interval: bool
        A flag defining whether the dataframe index should be reindexed to be *continuous* adding samples to each second
    fill_method: str, {{'ffill', 'bfill'}}
        requires `reindex_interval` to be `true`. Method to be used when reindexing the dataframe
    rolling_window: bool
        A string defining the rolling window to be applied. If this is not defined, no rolling window will be applied. (See (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    Returns
    -------
        Dataframe
            A resampled dataframe containing aggregated values for the given time window.
    """
    tmp_df = dataframe.copy()
    # reindex dataframe so that continuous values between intervals are added to the dataframe
    if reindex_interval:
        index = pd.DatetimeIndex(pd.date_range(start=pd.to_datetime(0), end=pd.to_datetime(86400 * SECOND_TO_NANO_SECOND), freq='S'))
        tmp_df = tmp_df.reindex(index, fill_value=np.NaN)
        limit = int(window.lower().removesuffix('min')) * 60
        tmp_df = tmp_df.fillna(method=fill_method, limit=limit)
        tmp_df = tmp_df.fillna(value=0)
    # add time as seconds as column
    tmp_df.index.name = 'time'
    tmp_df = tmp_df.reset_index()
    tmp_df[DEFAULT_TIME_STAMP_COLUMN] = (tmp_df['time'] - pd.to_datetime(0)).dt.total_seconds()
    tmp_df = tmp_df.set_index(['time'])
    # apply rolling window if enabled
    if rolling_window:
        tmp_df = tmp_df.rolling(window=rolling_window, closed='neither').mean()
    # remove samples outside the dataframe
    tmp_df = tmp_df.loc[(tmp_df.index >= pd.to_datetime(time_frame[0] * HOUR_TO_SECOND * SECOND_TO_NANO_SECOND))
                        & (tmp_df.index <= pd.to_datetime(time_frame[1] * HOUR_TO_SECOND * SECOND_TO_NANO_SECOND))]
    return tmp_df


def _resample_data_per_edge(resample_function: Callable, dataframe: pd.DataFrame, grouper, window: str = '15Min', reindex_interval: bool = False, fill_method: str = 'ffill',
                            time_frame: (float, float) = (0, 24), rolling_window: str = None) -> pd.DataFrame:
    """
    This internal helper method uses :func:`resample_function` to create a multi-indexed dataframe using the time and the edge id as index columns looking at each edge independently.

    Parameters
    ----------
    resample_function: Callable
        Function to be used for resampling of single-edged dataframes
    dataframe: pd.DataFrame
        a dataframe containing the read database table from an FCD Database
    grouper: dict[str, str]
        a dictionary defining the aggregation methods for different rows in the dataframe
    time_frame: (float, float)
        A tuple of defining the time frame in which the returned data should be to get data between 6am and 6pm use (6, 18)
    window: str
        a time string defining the aggregation window (see `here<https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_.)
    reindex_interval: bool
        A flag defining whether the dataframe index should be reindexed to be *continuous* adding samples to each second
    fill_method: str, {{'ffill', 'bfill'}}
        requires `reindex_interval` to be `true`. Method to be used when reindexing the dataframe
    rolling_window: bool
        A flag that enables rolling window aggregation
    Returns
    -------
        Dataframe
            A multi-indexed resampled dataframe containing aggregated values for the given time window for all edges defined within the given dataframe.
    """
    tmp_df = dataframe.copy()
    index_levels = ['connectionID', 'time']
    tmp_df.insert(0, 'time', pd.to_datetime(tmp_df[DEFAULT_TIME_STAMP_COLUMN]))
    tmp_df = tmp_df.drop(columns=[DEFAULT_TIME_STAMP_COLUMN])
    tmp_df = tmp_df.set_index(index_levels)  # create multi-indexed dataframe
    new_df = pd.DataFrame()
    for connection, group in tmp_df.groupby(level='connectionID'):
        current_connection_df = group.copy()
        current_connection_df = current_connection_df.reset_index(level='connectionID')
        current_connection_df = resample_function(current_connection_df, grouper=grouper, window=window, time_frame=time_frame, reindex_interval=reindex_interval,
                                                  fill_method=fill_method, rolling_window=rolling_window)
        current_connection_df = current_connection_df.reset_index()
        current_connection_df['connectionID'] = connection
        new_df = pd.concat([new_df, current_connection_df], axis=0)
    new_df = new_df.set_index(index_levels)
    return new_df
