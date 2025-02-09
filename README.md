# Data Modeling with Apache Cassandra

## Project Overview
This project involves building a **NoSQL database using Apache Cassandra** for **Sparkify**, a music streaming app. The database will allow querying user activity data efficiently. The project includes **data modeling, ETL processing, and querying**.

## Technologies Used
- **Apache Cassandra** - NoSQL database for fast, scalable queries.
- **Python** - ETL processing.
- **Pandas** - Data processing.
- **Jupyter Notebook** - Interactive environment for development.

## Data Modeling
### Tables Created:
1. **songplays_by_session** - Retrieve songplays by session and item in session.
2. **songplays_by_user** - Retrieve songplays by user and session.
3. **songplays_by_song** - Retrieve users who listened to a specific song.

### Primary Key Considerations:
- **Partition Keys** ensure efficient data distribution.
- **Clustering Keys** help in sorting and retrieval.

## ETL Pipeline
1. **Extract** - Read raw CSV data.
2. **Transform** - Process and format the data.
3. **Load** - Insert processed data into Cassandra tables.

## Running the Project
1. **Run `cassandra_db.py`** to set up the database.
2. **Run `etl.py`** to load data into the database.
3. **Run queries in `project_notebook.ipynb`** to validate the data.

## Example Query
Retrieve all songs played in a specific session:
```sql
SELECT artist, song_title FROM songplays_by_session WHERE session_id=123 AND item_in_session=1;
```
