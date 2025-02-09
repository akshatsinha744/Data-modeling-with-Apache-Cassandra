-- Drop existing tables
DROP TABLE IF EXISTS songplays_by_session;
DROP TABLE IF EXISTS songplays_by_user;
DROP TABLE IF EXISTS songplays_by_song;

-- Create tables
CREATE TABLE IF NOT EXISTS songplays_by_session (
    session_id INT, item_in_session INT, artist TEXT, song_title TEXT, 
    PRIMARY KEY (session_id, item_in_session)
);

CREATE TABLE IF NOT EXISTS songplays_by_user (
    user_id INT, session_id INT, artist TEXT, song_title TEXT, 
    PRIMARY KEY (user_id, session_id)
);

CREATE TABLE IF NOT EXISTS songplays_by_song (
    song_title TEXT, user_id INT, 
    PRIMARY KEY (song_title, user_id)
);
