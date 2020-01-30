import os
import glob
import json
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    # open song file
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS:
    * cur the cursor variable
    * filepath the file path to the song file

    OUTPUT:
    * song_data in songs table.
    * artist_data in artists table.
    """

    with open(filepath) as f:
       dt = json.load(f)

    df = pd.DataFrame(dt, index=['0'])

    # insert song record
    song_data = (df['song_id'][0], df['title'][0], df['artist_id'][0], int(df['year'][0]), df['duration'][0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = (df['artist_id'][0], df['artist_name'][0], df['artist_location'][0], df['artist_latitude'][0], df['artist_longitude'][0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the time, user and songplay information in order to store it into the time, user_data and songplay_data tables.

    INPUTS:
    * cur - the cursor variable
    * filepath - path to the log file to be processed

    OUTPUT:
    * time_data in time table.
    * user_data in users table.
    * songplay_data in songplay table.
    """

    df = pd.DataFrame(pd.read_json( filepath,
                                    lines=True,
                                    orient='columns'))

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = list(zip(   t.dt.strftime('%Y-%m-%d %I:%M:%S'),
                            t.dt.hour,
                            t.dt.day,
                            t.dt.week,
                            t.dt.month,
                            t.dt.year,
                            t.dt.weekday))
    column_labels = ('start_time',
                     'hour',
                     'day',
                     'week',
                     'month',
                     'year',
                     'weekday')
    time_df = pd.DataFrame( time_data,
                            columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates('userId', keep='last')

    #rename columns
    user_df.columns = ['user_id', 'first_name', 'last_name', 'gender',  'level']

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.to_datetime(
                            row.ts,
                            unit='ms').strftime(
                            '%Y-%m-%d %I:%M:%S')
        songplay_data = (   start_time,
                            row.userId,
                            row.level,
                            str(songid),
                            str(artistid),
                            row.sessionId,
                            row.location,
                            row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Get all files matching extension from directory

    INPUTS:
    * cur --        the cursor variable
    * conn --       connection to the db.
    * filepath --   path to file to be processed
    * func --       function to be called (process_song_data or
                    process_log_data)
    OUTPUT:
    * console printouts of the data processing.
    """
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
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Connect to DB and call process_data to walk through the input data (data/song_data and data/log_data).

    INPUTS:
    * None

    OUTPUT:
    * All input data is processed in DB tables.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
