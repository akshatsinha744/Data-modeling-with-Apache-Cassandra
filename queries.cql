DROP TABLE IF EXISTS songplays_by_session;
DROP TABLE IF EXISTS songplays_by_user;
DROP TABLE IF EXISTS songplays_by_song;

CREATE TABLE IF NOT EXISTS songplays_by_session (
    session_id INT, item_in_session INT, artist TEXT, song_title TEXT, song_length FLOAT,
    PRIMARY KEY (session_id, item_in_session)
);

CREATE TABLE IF NOT EXISTS songplays_by_user (
    user_id INT, session_id INT, item_in_session INT, artist TEXT, song_title TEXT, first_name TEXT, last_name TEXT,
    PRIMARY KEY ((user_id, session_id), item_in_session)
);

CREATE TABLE IF NOT EXISTS songplays_by_song (
    song_title TEXT, user_id INT, first_name TEXT, last_name TEXT,
    PRIMARY KEY (song_title, user_id)
);
