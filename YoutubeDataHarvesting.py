# Youtube data Harvesting 


from googleapiclient.discovery import build
import streamlit as st
import pymongo
import psycopg2
import isodate


api_key = 'AIzaSyBPiSMJ9CbfFfeNHELifr62u34wlGs-oww'
channel_id = 'UCG0m9a2z1ziRm2YlaFuyU7A' #Today Trending

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
    video_details =[]
    
    request = youtube.videos().list(part='snippet,statistics,contentDetails',id=video_ids)
    video_data = request.execute()
    for i in video_data['items']:
        video_information = {
                                'Video_Id': video_ids,
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
                                'Dislike_count': i['statistics']['dislikeCount'] if "dislikeCount" in video_data['items'][0]['statistics'] else "Not Available"}      
        video_details.append(video_information)  
    return video_details

        

def comment_details(video_ids):
    comments = []
    for i in video_ids:
        com_request = youtube.commentThreads().list(part="snippet,replies", videoId=i)
        comments_data = com_request.execute()
        for i in comments_data['items']:
            comment_information = {'Comment_Id':i["id"],
                            'Comment_Author':i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            'Comment_Text':i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            'Comment_PublishedAt':i["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                            'Video_Id':i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            'Rating':i["snippet"]["topLevelComment"]["snippet"]["viewerRating"],
                            'Comment_ch_id': i["snippet"]["topLevelComment"]["snippet"]["authorChannelId"],
                            'Comment_like_count':i["snippet"]["topLevelComment"]["snippet"] ["likeCount"],
                            'Comment_Reply_count':i["snippet"]["totalReplyCount"]}
         
            comments.append(comment_information)
    return comments


client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:Tn22ak7588@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_Project"]
col = db["Channel_Details"]


def mongo_data(input):
    channel_data= channel_details(input)
    playlist_ID = Video_ID(channel_data['Playlists_ID'])
    video_data= video_details(playlist_ID)
    comment_data = comment_details(playlist_ID)
    data = {'Channel_Details' :channel_data,
            'Video_ID': playlist_ID,
            'Video_details': video_data,
            'Comment_Details': comment_data
           }
    col.insert_one(data)
    return data



client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:Tn22ak7588@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
    
db = psycopg2.connect(host='localhost', user='postgres', password='Sandy@20', port=5432, database='guvi')
feed = db.cursor()


def retrive(data):

    client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:Tn22ak7588@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
    
    db = psycopg2.connect(host='localhost', user='postgres', password='Sandy@20', port=5432, database='guvi')
    feed = db.cursor()

    feed.execute("""create table if not exists channel(
    Channel_ID varchar unique,
    Channel_Name text,
    Subscription_Count int,
    Channel_views int,
    Channel_Videos int,
    Channel_Description text,
    Playlists_ID varchar)""")
    db.commit()

                 
    feed.execute("""create table if not exists video (
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

    feed.execute("""create table if not exists comment (
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


    channel_data = client["Youtube_Project"]["Channel_Details"].find_one({'channel_data.Channel_Name': data}, {"_id": 0})

    
    try:
        feed.execute("INSERT INTO channel VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                    (channel_data['channel']['Channel_ID'],channel_data['channel']['Channel_Name'], channel_data['channel']['Subscription_Count'],
                    channel_data['channel']['Channel_views'], channel_data['channel']['Channel_Videos'], channel_data['channel']['Channel_Description'],
                    channel_data['channel']['Playlists_ID']))
        

        db.commit()

        for vd in channel_data['Video_details']:
            feed.execute("INSERT INTO video VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (vd['Video_Id'], vd['channel_id'],
                                vd['channel_name'], vd['Video_Name'],
                                vd['Video_Description'], vd['Published_At'],
                                vd['video_duration'], vd['View_count'],
                                vd['Like_count'], vd['Comment_Count'],
                                vd['Favorite_Count'], vd['Dislike_count']))
            db.commit()
            for cd in channel_data['Comment_details']:
                feed.execute("INSERT INTO comment VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                            (cd['Comment_Author'], cd['Comment_Text'],
                            cd['Comment_PublishedAt'], cd['Video_Id'],
                            cd['Rating'], cd['Comment_ch_id'], cd['Comment_like_count'], cd['Comment_Reply_count']))
                db.commit()
            st.success("Data Migrated Succesfully to SQL Database.")
    except:
        st.error("Data Already Exists")

def Store():
     chanelname =[]
     client = pymongo.MongoClient("mongodb+srv://Santhosh_Kumar:Tn22ak7588@santhosh.foxesxo.mongodb.net/?retryWrites=true&w=majority")
     for i in client["Youtube_Project"]["Channel_Details"].find():
         chanelname.append(i["Channel_Details"]["Channel_Name"])
     return chanelname


def answers(data):
    if data == "Visualisation of video names along with their channel name":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select channel_name ,Video_Name from video")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"channel_name",2:"Video_Name"}))
        except:
            st.text("Error executing SQL query")

    if data == "Visualisation of channels that have most number of videos along with their video count":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select Channel_Name ,Channel_Videos from channel order by Channel_Videos desc limit 5 ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name", 2:"Total Number of Videos"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of top 10 most viewed videos and their respective channels":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select Video_Name ,View_count from video order by View_count desc limit 10 ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Video Name", 2:"Total Views"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of no of comments made on each video along with their corresponding video names":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select channel_name ,Video_Name , Comment_Count from video order by channel_name, Video_Name ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name",2: "Video Name", 3:"Video Comment Count"}))
        except:
            st.text("Error executing SQL query")
        
    if data == "Visualisation of videos that have the highest number of likes along their corresponding channel name.":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select channel_name, Video_Name ,Like_count from video order by Like_count desc limit 15 ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name",2:"Video Name",3:"Video Likes Count"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of total number of likes and dislikes for each video along with their corresponding video names":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select Video_Name ,Like_count ,Dislike_count from video")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Video Name", 2:"Video Likes", 3:"Video Dislikes"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of total number of views for each channel along with their corresponding channel names":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select Channel_Name ,Channel_views from channel order by Channel_Name")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name", 2:"Channel Views"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of names of all the channels that have published videos in the year 2022":
        try: 
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select distinct channel_name from video where Published_At like '2022%' ")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name"}))
        except:
            st.text("Error executing SQL query")
        
    
    if data == "Visualisation of Average duration of all videos in each channel along with their corresponding channel name.":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select channel_name , avg(video_duration) from video group by channel_name")
            result = exe.fetchall()
            res = []
            for i in result:
                res.append(i)
            st.dataframe(res,width= 5000,column_config=({1:"Channel Name", 2:"Average Video Duration in hours"}))
        except:
            st.text("Error executing SQL query")
    
    if data == "Visualisation of Videos that have highest number of comments and their corresponding channel name.":
        try:
            db = psycopg2.connect(host = 'localhost',user = 'postgres',password='Sandy@20',port = 5432,database = 'guvi')
            exe = db.cursor()
            exe.execute("select channel_name ,Video_Name ,Comment_Count from video order by Comment_Count desc limit 10 ")
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