import pandas as pd
import cassandra
from cassandra.cluster import Cluster

def process_data(file_path):
    """Reads CSV data and processes it into a DataFrame."""
    df = pd.read_csv(file_path)
    return df

def insert_data(session, df):
    """Inserts processed data into Cassandra tables."""
    for _, row in df.iterrows():
        session.execute("INSERT INTO songplays_by_session (session_id, item_in_session, artist, song_title) VALUES (%s, %s, %s, %s)",
                        (row.sessionId, row.itemInSession, row.artist, row.song))

def main():
    """Main ETL function to extract, transform, and load data."""
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('sparkify')

    df = process_data('event_datafile_new.csv')
    insert_data(session, df)

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
