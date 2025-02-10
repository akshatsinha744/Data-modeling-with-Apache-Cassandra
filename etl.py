import pandas as pd
import cassandra
from cassandra.cluster import Cluster

def process_data(file_path):
    df = pd.read_csv(file_path)
    return df

def insert_data(session, df):
    for _, row in df.iterrows():
        session.execute("""
            INSERT INTO songplays_by_session (session_id, item_in_session, artist, song_title, song_length)
            VALUES (%s, %s, %s, %s, %s)
        """, (row.sessionId, row.itemInSession, row.artist, row.song, row.length))
        
        session.execute("""
            INSERT INTO songplays_by_user (user_id, session_id, item_in_session, artist, song_title, first_name, last_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row.userId, row.sessionId, row.itemInSession, row.artist, row.song, row.firstName, row.lastName))
        
        session.execute("""
            INSERT INTO songplays_by_song (song_title, user_id, first_name, last_name)
            VALUES (%s, %s, %s, %s)
        """, (row.song, row.userId, row.firstName, row.lastName))

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('sparkify')

    df = process_data('event_datafile_new.csv')
    insert_data(session, df)

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
