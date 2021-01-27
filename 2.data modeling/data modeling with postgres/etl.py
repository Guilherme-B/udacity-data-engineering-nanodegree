import os
import glob
import psycopg2
import psycopg2.extras
import pandas as pd

from pathlib import Path
from typing import Callable, List, Optional, Union, Tuple

from sql_queries import *



def insert_dataframe(cursor: psycopg2.extensions.cursor, df: pd.DataFrame, query: str, fields: List[str] = None, batch_mode: bool = True) -> bool:
    """Inserts the contents of a pandas DataFrame into a PostgreSQL database

    Args:
        cursor (psycopg2.extensions.cursor): The active cursor used to insert the data
        df (pd.DataFrame): The pandas DataFrame data container
        query (str): The Psycopg2 compliant query to be executed
        fields (list[str], optional): The column subset to be considered by df. Defaults to None.
        batch_mode (bool, optional): True to use batch mode (optimized for multi-inserts). Defaults to True.

    Returns:
        bool: True if the process completed properly
    """    
    
    current_file_name: str = os.path.basename(__file__)
    
    if cursor is None:
        print(current_file_name, '::insert_dataframe no cursor provided.')
        return False
    
    if df is None:
        print(current_file_name, '::insert_dataframe no pandas.DataFrame provided.')
        return False
    
    if query is None:
        print(current_file_name, '::insert_dataframe no query provided.')
        return False
    
    try:
        # Use the provided fields, if any
        data = df if fields is None else df[fields]

        if batch_mode:
            # Fetch the data and load it into a List of Tuples
            data = df.values.tolist()
            data = [tuple(row) for row in data]

            cursor.executemany(query, data)
        else:
            for i, row in df.iterrows():
                cursor.execute(query, list(row))
    except Exception as err:
        print(current_file_name, '::insert_dataframe {error}'.format(error=err))
        
        return False
    
    return True

def expand_time(df: pd.DataFrame, time_field: str) -> Optional[Union[pd.DataFrame, pd.Series]]:
    """Expands and returns a pandas DataFrame's time field in milliseconds extracting time units:
        * year
        * month
        * day
        * hour
        * weekday_name
        * week

    Args:
        df (pd.DataFrame): The pandas DataFrame to expand
        time_field (str): Name of the time field/column to expand

    Returns:
        Optional[pd.DataFrame, pd.Series]: Returns None if an invalid argument is detected, else a Pandas DataFrame or Series contingent on the number of elements
    """    
    
    current_file_name: str = os.path.basename(__file__)
    
    if df is None:
        print(current_file_name, '::insert_dataframe no dataframe provided.')
        return None
    
    if time_field is None:
        print(current_file_name, '::insert_dataframe no timestamp column provided.')
        return None
    
    df[time_field] = pd.to_datetime(df[time_field], unit = 'ms')
    
    t = df

    t['year'] = df.ts.dt.year
    t['month'] = df.ts.dt.month
    t['day'] = df.ts.dt.day
    t['hour'] = df.ts.dt.hour
    t['weekday_name'] = df.ts.dt.weekday_name
    t['week'] = df.ts.dt.week
    
    return t

def process_song_file(cur: psycopg2.extensions.cursor, filepath: str) -> bool:
    """ Processes the song files contained in @filepath populating the User and Time Dimensions and calling @process_songplay

    Args:
        cur (psycopg2.extensions.cursor): The active connection cursor used to insert the data
        filepath (str): The filepath containing the log files

    Returns:
        bool: True if the processing and insertion was succesful
    """    
    
    if not Path(filepath).is_file():
        current_file_name: str = os.path.basename(__file__)

        print(current_file_name, '::process_song_file aborting, log file not found, {folder}.'.format(folder= filepath))
        
        return False
    
    # open song file
    df = pd.read_json(filepath, lines= True)

    success_flag: bool = False
    
    # process and insert sond records
    song_data = df[['song_id', 'title', 'artist_id', 'year','duration']]
    success_flag = insert_dataframe(cursor=cur, df=song_data, query=song_table_insert)
    
    
    if not success_flag:
        return success_flag
    
    # process and insert artist records
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    success_flag = insert_dataframe(cursor=cur, df=artist_data, query=artist_table_insert)
    
    return success_flag
    
def get_song_artist(cur: psycopg2.extensions.cursor, song: str, artist: str, length: float) -> Optional[Tuple[str, str]]:
    """Fetches the Song ID and Artist ID associated with the provided song (name), artist (name) and song length.

    Args:
        cur (psycopg2.extensions.cursor): The active cursor used to fetch the data
        song (str): The name of the song to search
        artist (str): The name of the artist to search
        length (float): The length of the song to be searched

    Returns:
        Optional[Tuple[str, str]]: None if the arguments are invalid, or a Tuple containing the retrieved Song ID and Artist ID
    """    
    
    # get songid and artistid from song and artist tables
    cur.execute(song_select, (song, artist, length))
    results = cur.fetchone()
    
    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None
        
    return songid, artistid
    
def process_songplay(cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> bool:
    """ Processes the song data from a pandas DataFrame and inserts it into the Songplay Fact table

    Args:
        cur (psycopg2.extensions.cursor): The active cursor used to insert the data
        df (pd.DataFrame): The pandas Dataframe containing the required fields (see notes)

    Returns:
        bool: True if the operation was succesful
        
    Notes:
        The required fields to be included in the pandas DataFrame are:
            * ts
            * userID
            * level
            * song
            * artist
            * length
            * sessionId
            * location
            * userAgent
    """    
    
    for _, row in df.iterrows():
        
        try:
            # get songid and artistid from song and artist tables
            songid, artistid = get_song_artist(cur, row.song, row.artist, row.length)

            # insert songplay record
            songplay_data = (row.ts,
                            row.userId,
                            row.level,
                            songid,
                            artistid,
                            row.sessionId,
                            row.location,
                            row.userAgent                 
                            )

            cur.execute(songplay_table_insert, songplay_data)
        except Exception as err:
            current_file_name: str = os.path.basename(__file__)

            print(current_file_name, '::process_songplay record not inserted, {error}.'.format(error= err))
            
            return False
        
        
    return True


def process_log_file(cur: psycopg2.extensions.cursor, filepath: str) -> bool:
    """ Processes the log files contained in @filepath populating the User and Time Dimensions and calling @process_songplay

    Args:
        cur (psycopg2.extensions.cursor): The active connection cursor used to insert the data
        filepath (str): The filepath containing the log files
        
     Returns:
        bool: True if the processing and insertion was succesful
    """    
    
    if not Path(filepath).is_file():
        current_file_name: str = __file__

        print(current_file_name, '::process_log_file aborting, log folder not found, {folder}.'.format(folder= filepath))
        
        return False
    
    # open log file
    df = pd.read_json(filepath, lines= True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # retrieve time-related fields
    t = expand_time(df, 'ts')
    
    # insert time data records
    column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday_name']
    time_df = t[column_labels]
    
    # Holds the operation's status
    success_flag: bool = False
    
    success_flag = insert_dataframe(cursor= cur, df= time_df, query= time_table_insert)

    if not success_flag:
        return success_flag
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    success_flag = insert_dataframe(cursor= cur, df= user_df, query= user_table_insert)
    
    if not success_flag:
        return success_flag

    # insert songplay records
    success_flag = process_songplay(cur, df)
    
    return success_flag


def process_data(cur: psycopg2.extensions.cursor, conn, filepath: str, func: Callable[..., bool]):
    """Processes the JSON files in the provided @filepath calling the corresponding functions to process the data

    Args:
        cur (psycopg2.extensions.cursor): The active psycopg2.extensions.cursor used to insert the data
        conn (psycop2.connection): The active psycopg2 connection
        filepath (str): The OS directory containing the JSON files to be processed
        func (Callable[...]): The function to be called for each retrieved file
    """    
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        operation_status: bool = func(cur, datafile)
        
        if operation_status:
            conn.commit()
        else:
            conn.rollback()
            
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()