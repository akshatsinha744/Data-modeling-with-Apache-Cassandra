from cassandra.cluster import Cluster

def create_keyspace():
    """Creates Cassandra keyspace for the project."""
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify 
        WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
    """)
    session.set_keyspace('sparkify')
    return session

def create_tables(session):
    """Creates necessary tables for queries."""
    session.execute("""
        CREATE TABLE IF NOT EXISTS songplays_by_session (
            session_id INT, item_in_session INT, artist TEXT, song_title TEXT, song_length FLOAT,
            PRIMARY KEY (session_id, item_in_session)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS songplays_by_user (
            user_id INT, session_id INT, item_in_session INT, artist TEXT, song_title TEXT, first_name TEXT, last_name TEXT,
            PRIMARY KEY ((user_id, session_id), item_in_session)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS songplays_by_song (
            song_title TEXT, user_id INT, first_name TEXT, last_name TEXT,
            PRIMARY KEY (song_title, user_id)
        );
    """)

def main():
    """Main function to create database and tables."""
    session = create_keyspace()
    create_tables(session)
    session.shutdown()

if __name__ == "__main__":
    main()
