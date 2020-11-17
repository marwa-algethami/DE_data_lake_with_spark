### Data Lake setup using Spark on S3

**About

This project aims to Analyze a startup called Sparkify they've been collecting on songs and user activity on their new music streaming app.Their data resides in S3,in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 
 My role was to building ETL pipeline to extract data from S3 and process them using Spark and then load back to S3 in the fact and dimension tables.



**Database design**  

**Fact Table:  

- songplays - The fact table. with following attributes  
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**  

- users -Dimensional table extracted from log_data files. with the following attributes user_id, first_name, last_name, gender, level
- songs -Dimensional table extracted from song_data files. With the following attributes song_id, title, artist_id, year, duration
- artists -Dimensional table extracted from song_data files. With the following attributes artist_id, name, location, latitude, longitude 
- time -Dimensional table extracted from Timestamp column. with the following attributes start_time, hour, day, week, month, year, weekday

**ETL pipeline 

- Loading valid credintials in dl.cfg
- Read data from s3 bucket
- Transform data by creating five tables
- Load data back to a new s3 bucket  

**Output

- Songs table files are partitioned by year and then artist.
- Time table files are partitioned by year and month. 
- Songplays table files are partitioned by year and month.

**Run Pipeline  

 - In the terminal shell writye python etl.py then wait until the processing is completed