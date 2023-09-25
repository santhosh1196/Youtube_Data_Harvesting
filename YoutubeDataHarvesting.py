# Youtube data Harvesting 


from googleapiclient.discovery import build
import streamlit as st
import pymongo
import psycopg2
import isodate

api_key = 'AIzaSyBPiSMJ9CbfFfeNHELifr62u34wlGs-oww'
#channel_id = 'UCG0m9a2z1ziRm2YlaFuyU7A' #Today Trending

youtube = build('youtube', 'v3', developerKey=api_key)

def duration(data):
    dur = isodate.parse_duration(data)
    sec = dur.total_seconds()
    hours = float(int(sec) / 3600)
    return hours


def channel_details(channel_id):
    response = youtube.channels().list(id=channel_id, part='snippet,statistics,contentDetails')
    channel_data = response.execute()
    for i in channel_data['items']:
        channel_informations = {'Channel_ID' : i['id'],
                                'Channel_Name' : i['snippet']['title'],
                                'Subscription_Count': i['statistics']['subscriberCount'],
                                'Channel_views': i['statistics']['viewCount'],
                                'Channel_Videos': i['statistics']['videoCount'],
                                'Channel_Description' : i['snippet']['description'],
                                'Playlists_ID' : i['contentDetails']['relatedPlaylists']['uploads']}
        return channel_informations


def Video_ID(Playlists_ID):
    video_id = []
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(part="contentDetails", playlistId=Playlists_ID, maxResults=50, pageToken=next_page_token)
        response = request.execute()
        for i in response['items']:        
            video_id.append(i['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        if next_page_token is not None:
            break
    return video_id


def video_details(video_ids):
    v_details =[]
    for j in video_ids:

        request = youtube.videos().list(part='snippet,statistics,contentDetails',id=j)
        video_data = request.execute()
        for i in video_data['items']:
            video_information = {
                                    'Video_Id': j,
                                    'channel_id' : i["snippet"]["channelId"],
                                    'channel_name' : i["snippet"]["channelTitle"],
                                    'Video_Name': i['snippet']['title'] if 'title' in video_data['items'][0]['snippet'] else "Not Available",
                                    'Video_Description': i['snippet']['description'],
                                    'Published_At': i['snippet']['publishedAt'],
                                    'video_duration' :duration(i["contentDetails"]["duration"]),
                                    'View_count': i['statistics']['viewCount'],
                                    'Like_count': i['statistics']['likeCount'],
                                    'Comment_Count': i['statistics']['commentCount'],
                                    'Favorite_Count': i['statistics']['favoriteCount'],
                                    'Dislike_count': i['statistics']['dislikeCount'] if "dislikeCount" in video_data['items'][0]['statistics'] else 0
                                    }      
            v_details.append(video_information)  
    return v_details

def comment_details(video_ids):
    comments = []
    for i in video_ids:
        com_request = youtube.commentThreads().list(part="snippet,replies", videoId=i)
        comments_data = com_request.execute()
        for j in comments_data['items']:
            comment_information = {'Comment_Id':j["id"],
                            'Comment_Author':j["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            'Comment_Text':j["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            'Comment_PublishedAt':j["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                            'Video_Id':j["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            'Rating':j["snippet"]["topLevelComment"]["snippet"]["viewerRating"],
                            'Comment_ch_id': j["snippet"]["topLevelComment"]["snippet"]["authorChannelId"],
                            'Comment_like_count':j["snippet"]["topLevelComment"]["snippet"] ["likeCount"],
                            'Comment_Reply_count':j["snippet"]["totalReplyCount"]}
         
            comments.append(comment_information)
    return comments


client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:San123@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_Project"]
col = db["Channel_Details"]


def mongo_data(input):
    channel_data= channel_details(input)
    v_ids = Video_ID(channel_data['Playlists_ID'])
    video_data= video_details(v_ids)
    comment_data = comment_details(v_ids)
    data = {'Channel_Details' :channel_data,
            'Video_ID': v_ids,
            'Video_Details': video_data,
            'Comment_Details': comment_data
           }
    col.insert_one(data)
    return data



client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:San123@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
    
db = psycopg2.connect(host='localhost', user='postgres', password='Sandy@20', port=5432, database='Youtube')
feed = db.cursor()


def retrive(data):

    client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:San123@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
    
    db = psycopg2.connect(host='localhost', user='postgres', password='Sandy@20', port=5432, database='Youtube')
    feed = db.cursor()

    feed.execute("""create table if not exists channel_table(
    Channel_ID varchar unique,
    Channel_Name text,
    Subscription_Count int,
    Channel_views int,
    Channel_Videos int,
    Channel_Description text,
    Playlists_ID varchar)""")
    db.commit()

                 
    feed.execute("""create table if not exists video_table (
    Video_Id varchar unique,
    channel_id varchar,
    channel_name text,
    Video_Name text,
    Video_Description text,
    Published_At varchar,
    video_duration float,
    View_count int,
    Like_count int,
    Comment_Count int,
    Favorite_Count int,
    Dislike_count int)""")
    db.commit()

    feed.execute("""create table if not exists comment_table (
    Comment_Id varchar unique,
    Comment_Author text,
    Comment_Text text,
    Comment_PublishedAt varchar,
    Video_Id varchar,
    Rating varchar,
    Comment_ch_id varchar,
    Comment_like_count int,
    Comment_Reply_count int)""")
    db.commit()

    col1 = client["Youtube_Project"]["Channel_Details"]
    channel_data = col1.find_one({'Channel_Details.Channel_Name': data},{"_id": 0})
    try:
        feed.execute("INSERT INTO channel_table VALUES(%s,%s,%s,%s,%s,%s,%s)",
                            (channel_data['Channel_Details']['Channel_ID'],channel_data['Channel_Details']['Channel_Name'], channel_data['Channel_Details']['Subscription_Count'],
                        channel_data['Channel_Details']['Channel_views'], channel_data['Channel_Details']['Channel_Videos'], channel_data['Channel_Details']['Channel_Description'],
                            channel_data['Channel_Details']['Playlists_ID']));
        try:
                db.commit()
        except:
                db.rollback()
        for i in channel_data['Video_Details']:
            feed.execute("INSERT INTO video_table VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                    (i['Video_Id'], i['channel_id'],
                                        i['channel_name'], i['Video_Name'],
                                        i['Video_Description'], i['Published_At'],
                                        i['video_duration'], i['View_count'],
                                        i['Like_count'], i['Comment_Count'],
                                        i['Favorite_Count'], i['Dislike_count']));
            try:
                db.commit()
            except:
                db.rollback()


        for j in channel_data['Comment_Details']:
            feed.execute("INSERT INTO comment_table VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                    (j['Comment_Id'],j['Comment_Author'], j['Comment_Text'],
                                    j['Comment_PublishedAt'], j['Video_Id'],
                                    j['Rating'], j['Comment_ch_id']['value'], 
                                    j['Comment_like_count'], j['Comment_Reply_count']));
            try:
                db.commit()
            except:
                db.rollback()
        st.success("Data Migrated to SQL")
    except:
        st.error("Data Already Exists")

def Store():
     chanelname =[]
     client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:San123@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
     for i in client["Youtube_Project"]["Channel_Details"].find():
         chanelname.append(i["Channel_Details"]["Channel_Name"])
     return chanelname


def answers(data):
    if data == "Visualisation of video names along with their channel name":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select Video_Name ,channel_name from video_table")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Video_Name",2:"channel_name"}))
        except:
            st.text("Error executing SQL query")

    if data == "Visualisation of channels that have most number of videos along with their video count":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select Channel_Name ,Channel_Videos from channel_table order by Channel_Videos desc limit 5 ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name", 2:"Total Number of Videos"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of top 10 most viewed videos and their respective channels":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select Video_Name ,View_count from video_table order by View_count desc limit 10 ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Video Name", 2:"Total Views"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of no of comments made on each video along with their corresponding video names":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select channel_name ,Video_Name , Comment_Count from video_table order by channel_name, Video_Name ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name",2: "Video Name", 3:"Video Comment Count"}))
        except:
            st.text("Error executing SQL query")
        
    if data == "Visualisation of videos that have the highest number of likes along their corresponding channel name.":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select channel_name, Video_Name ,Like_count from video_table order by Like_count desc limit 15 ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name",2:"Video Name",3:"Video Likes Count"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of total number of likes and dislikes for each video along with their corresponding video names":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select Video_Name ,Like_count ,Dislike_count from video_table")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Video Name", 2:"Video Likes", 3:"Video Dislikes"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of total number of views for each channel along with their corresponding channel names":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select Channel_Name ,Channel_views from channel_table order by Channel_Name")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name", 2:"Channel Views"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of names of all the channels that have published videos in the year 2022":
        try: 
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select distinct channel_name from video_table where Published_At like '2022%' ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name"}))
        except:
            st.text("Error executing SQL query")
        
    
    if data == "Visualisation of Average duration of all videos in each channel along with their corresponding channel name.":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select channel_name , avg(video_duration) from video_table group by channel_name")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name", 2:"Average Video Duration in hours"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of Videos that have highest number of comments and their corresponding channel name.":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'Youtube')
            exe = db.cursor()
            exe.execute("select channel_name ,Video_Name ,Comment_Count from video_table order by Comment_Count desc limit 10 ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name", 2:"Video Name", 3:"Comment Count"}))
        except:
            st.text("Error executing SQL query")
    

st.set_page_config(page_title="Youtube Data Harvesting and Warehousing",page_icon="▶️",layout="wide")
st.header(':rainbow[Youtube Data Harvesting and Warehousing]')
st.text("""
""")
col1, col2 =  st.columns(2)
with col1:
    st.markdown(":rainbow[Data Collection]")
    st.caption(":gray[This block is to fetch the data from youtube API and to upload the data into MongoDb Database]")
    channel_id = st.text_input(":gray[Enter Channel ID]")
    submit_1 = st.button("Upload",disabled=False)
    if submit_1:
       mongo_data(channel_id)

with col2:
    st.markdown(":rainbow[Data Migration]")
    st.caption(":gray[This block is to retrieve the data from the MongoDb DataBase and migrate it to SQL Database]")
    Channel_Name = st.selectbox('Select for data migration',options = Store())
    submit_2 = st.button("Migrate",disabled=False)
    if submit_2:
        retrive(Channel_Name)

Questions = [
        "Visualisation of video names along with their channel name",
        "Visualisation of channels that have most number of videos along with their video count",
        "Visualisation of top 10 most viewed videos and their respective channels",
        "Visualisation of no of comments made on each video along with their corresponding video names",
        "Visualisation of videos that have the highest number of likes along their corresponding channel name.",
        "Visualisation of total number of likes and dislikes for each video along with their corresponding video names",
        "Visualisation of total number of views for each channel along with their corresponding channel names",
        "Visualisation of names of all the channels that have published videos in the year 2022",
        "Visualisation of Average duration of all videos in each channel along with their corresponding channel name.",
        "Visualisation of Videos that have highest number of comments and their corresponding channel name."]

st.markdown(":rainbow[Data Analysis]")
st.caption(":gray[This block is to Analyse and visualize the Data that is Extracted]")
channel_name = st.selectbox('Select for Analysis',Questions)
submit_3 = st.button("Execute",disabled=False)
if submit_3:
    answers(channel_name)
