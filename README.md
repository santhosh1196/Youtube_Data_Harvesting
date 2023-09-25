# Youtube_Data_Harvesting

In This Project we would get YouTube Channel data from YouTube API with the help of 'Channel ID' , We Will Store the channel data into Mongo DB Atlas as a Document then the data Would convert into Sql Records for Data Analysis and then we retrive all the stored data in Stremlit application.

# Used Application

Visual Studio Code

MongoDB

PostGresql

Streamlit

# Skills Covered

Python Scripting

Data collection

API Integration

SQL

MongoDB

Streamlit 

Explanation To Do Execute The Project*

Step 1: We will fetch datas like Channel Details, Video Details, Comment Details from the Youtube data API console. 
        the flow of getting data --> Channel Details -->Channel ID --> Playlist ID --> Video Ids --> Video Details of each video 
                                                                                                 --> Comment Details of each video 
                        
Step2: Create a database in MongoDB to store the collected data as data lake.

Step3: Migration of data from data lake to Postgresql for efficient querying and analysis by creating tables in SQL warehouse.

Step4: A Stremlite application is created and all the extracted data will be visulized through the App.

How to Run: streamlit run main.py
