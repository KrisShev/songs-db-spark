#Goal
This project aims at creating database for Sparkify in spark. 

#How to run?
Add AWS credintials to the dl.cfg file and run etl.py. 
This opens a spark connection where json files from s3 bucket udacity-dend/song_data is used to create two parquet files - songs and artists. Afterwards json files from s3 bucket udacity-dend/log_data is used to filter out only records with page='NextSong'. Relevant time attributes are derives such as year, month, week, day, hour, weekday and saved into parquet file time. Another parquet files from log data is made  - users. Last, songplay parquet file is created from the combination between songs parquet file and log data.   

#File descriptions
* dwh.cfg contains all configuration parameters and access keys
* create_tables.py contains functions to create all database tables
* sql_queries.py contains all queries to drop and create tables, insert and copy statements  
* etl.py is wrapper that runs all of the above 

#Star Schema description
The star schema represents 4 fact tables and one dimensional table. The fact tables are easily combined to the dimensional table on primary keys.
* users - fact table that connects to songplays by user_id
* songs- fact table that connects to songplays by song_id
* artists- fact table that connects to songplays by artist_id
* time- fact table that connects to songplays by start_time
* songplays - dimenstional table that contains all songplays by active and logged-in users


#Why should Sparkify use this ETL?
By using this star database schema, Sparkify can easily and intiutively analyze its users and their behavior. It can answer questions are which week days and day hours 
are the most active and which are the most popular songs and artists.