from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pymongo
import psycopg2
import re
import streamlit as st
import json
import pandas as pd

#youtube client connection
api_key = 'enter your api key' # https://console.cloud.google.com/apis/credentials?project=youtube-api-project-386610
youtube = build('youtube', 'v3', developerKey=api_key)

##MongoDB client connection
conn1=pymongo.MongoClient("Enter the Mongodb client") # https://cloud.mongodb.com/v2/64338cc513c80f7e8be73db8#/clusters
db = conn1["Enter the database name"] 
coll = db["Enter the collection name"]


#SQL client connection
conn=psycopg2.connect(host="Enter the host name",
                      user="Enter the user name",
                      password="Enter the  password",
                      port="Enter the port" ,
                      database="Enter the database name")
cursor=conn.cursor()



def get_complete_channel_details(youtube, channel_id): 
    all_data = []
    
    # To get the channel details:
    request  = youtube.channels().list(
                       part='snippet,contentDetails,statistics',
                       id=channel_id)
    response = request.execute()

    for i in response["items"]:
        data = {"Channel_Name": {"Channel_Name": i['snippet']['title'],
                                 "Channel_id":   i["id"],
                                 "Subscription_Count": i['statistics']['subscriberCount'],
                                 "Channel_Views": i['statistics']['viewCount'],
                                 "Channel_Description": i['snippet']['description'],
                                 "playlist_id": i['contentDetails']['relatedPlaylists']['uploads']}}
        all_data.append(data)
        playlist_id = data["Channel_Name"].get("playlist_id")

        # to get the playlist_id details :
        request  = youtube.playlistItems().list(
                           part='contentDetails',
                           playlistId=playlist_id,
                           maxResults=50)
        response = request.execute()

        a = 1
        for i in response['items']:
            
            video_ids1 = i['contentDetails']['videoId']
           
            # to get the Video details :
            request   = youtube.videos().list(
                                part="snippet,contentDetails,statistics",
                                id=video_ids1)
            response1 = request.execute()
            
            try:
 
                # to get the comment details :
                request   = youtube.commentThreads().list(
                                  part="snippet,replies",
                                  videoId=video_ids1
                                  )
                response2 = request.execute()
                
            except HttpError:
                continue
            
            comments = []

            for comment in range(len(response2["items"])):
                
                    Comment_id = response2["items"][comment]["snippet"]["topLevelComment"].get("id",None),
                    comment_text = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"].get("textOriginal",None),
                    comment_Author = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"].get("authorDisplayName",None),
                    Comment_PublishedAt = response2["items"][comment]["snippet"]["topLevelComment"]["snippet"].get("publishedAt",None)
                    data = {f"comments_id_{comment+1}": {"Comment_id": Comment_id[0],
                                                         "comment_text": comment_text, "comment_Author": comment_Author[0],
                                                         "Comment_PublishedAt": Comment_PublishedAt}}
                    comments.append(data)


            if "items" in response1:
                video_stats = {f"video_id_{a}": dict(Video_Id=response1['items'][0].get("id", None),
                               Video_Name=response1['items'][0]['snippet'].get(
                                   'title', None),
                               PublishedAt=response1['items'][0]['snippet'].get(
                                   'publishedAt', None),
                               Video_Description=response1['items'][0]['snippet'].get(
                                   'description', None),
                               Tags=response1['items'][0]['snippet'].get(
                                   'tags', None),
                               View_Count=response1['items'][0]['statistics'].get(
                                   'viewCount', 0),
                               Like_Count=response1['items'][0]['statistics'].get(
                                   'likeCount', 0),
                               Comment_Count=response1['items'][0]['statistics'].get(
                                   'commentCount', 0),
                               Favorite_Count=response1['items'][0]['statistics'].get(
                                   'favoriteCount', 0),
                               Duration=response1['items'][0]['contentDetails'].get(
                                   'duration', None),
                               Thumbnail=response1['items'][0]['snippet']['thumbnails']['default'].get(
                                   'url', None),
                               Caption_Status=response1['items'][0]['contentDetails'].get(
                                   'caption', None),
                               Comments=comments)}
                all_data.append(video_stats)
                next_page_token = response.get("nextPageToken")
                a = a+1
            else:
                continue

            while next_page_token is not None:
                request  = youtube.playlistItems().list(
                                  part="contentDetails",
                                  maxResults=50,
                                  playlistId=playlist_id,
                                  pageToken=next_page_token
                                  )
                response = request.execute()

              
                for i in response['items']:
                    video_ids1 = i['contentDetails']['videoId']
                    request   = youtube.videos().list(
                                      part="snippet,contentDetails,statistics",
                                      id=video_ids1)
                    response4 = request.execute()
                    
                    try:

                        request  = youtube.commentThreads().list(
                                          part="snippet,replies",
                                          videoId=video_ids1
                                          )
                        response5 = request.execute()
                    except HttpError:
                        continue
                    
                    comments = []

                    for comment in range(len(response5["items"])):
                        
                            Comment_id = response5["items"][comment]["snippet"]["topLevelComment"].get("id",None),
                            comment_text = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"].get("textOriginal",None),
                            comment_Author = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"].get("authorDisplayName",None),
                            Comment_PublishedAt = response5["items"][comment]["snippet"]["topLevelComment"]["snippet"].get("publishedAt",None)
                            data = {f"comments_id_{comment+1}": {"Comment_id": Comment_id[0],
                                                                 "comment_text": comment_text, "comment_Author": comment_Author[0],
                                                                 "Comment_PublishedAt": Comment_PublishedAt}}
                            comments.append(data)
                       

                    if "items" in response4:
                        video_stats = {f"video_id_{a}": dict(Video_Id=response4['items'][0].get("id", None),
                                                             Video_Name=response4['items'][0]['snippet'].get(
                                                                 'title', None),
                                                             PublishedAt=response4['items'][0]['snippet'].get(
                                                                 'publishedAt', None),
                                                             Video_Description=response4['items'][0]['snippet'].get(
                                                                 'description', None),
                                                             Tags=response4['items'][0]['snippet'].get(
                                                                 'tags', None),
                                                             View_Count=response4['items'][0]['statistics'].get(
                                                                 'viewCount', 0),
                                                             Like_Count=response4['items'][0]['statistics'].get(
                                                                 'likeCount', 0),
                                                             Comment_Count=response4['items'][0]['statistics'].get(
                                                                 'commentCount', 0),
                                                             Favorite_Count=response4['items'][0]['statistics'].get(
                                                                 'favoriteCount', 0),
                                                             Duration=response4['items'][0]['contentDetails'].get(
                                                                 'duration', None),
                                                             Thumbnail=response4['items'][0]['snippet']['thumbnails']['default'].get(
                                                                 'url', None),
                                                             Caption_Status=response4['items'][0]['contentDetails'].get(
                                                                 'caption', None),
                            Comments=comments)}
                        all_data.append(video_stats)
                        next_page_token = response.get("nextPageToken")
                        a = a+1
                    else:
                        continue

    list_of_dicts = all_data
    result_dict = {key: value for d in list_of_dicts for key, value in d.items()}

    return result_dict



def migrate_to_sql(import_from_mongodb):
    
    #to migrate channel details to channel tebel
    
    df_channel_details = pd.DataFrame(columns=["channel_id", "channel_name", "subscription_count", "channel_views", "channel_description"])
    
    df_channel_details.loc[0] = [import_from_mongodb[0]["Channel_Name"]["Channel_id"],
                                 import_from_mongodb[0]["Channel_Name"]["Channel_Name"],
                                 import_from_mongodb[0]["Channel_Name"]["Subscription_Count"],
                                 import_from_mongodb[0]["Channel_Name"]["Channel_Views"],
                                 import_from_mongodb[0]["Channel_Name"]["Channel_Description"]]

    df_channel_details["subscription_count"] = pd.to_numeric(df_channel_details["subscription_count"])
    df_channel_details["channel_views"] = pd.to_numeric(df_channel_details["channel_views"])

    values = df_channel_details.values.tolist()
    cursor = conn.cursor()

    query = "INSERT INTO channel (channel_id, channel_name, subscription_count, channel_views, channel_description) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(query, values)

    conn.commit()
    
    #to migrate playlist_id details to playlist_id teble:
    
    df_playlist_details = pd.DataFrame(columns=["channel_id", "playlist_id"])
    
    df_playlist_details.loc[0] = [import_from_mongodb[0]["Channel_Name"]["Channel_id"],
                                  import_from_mongodb[0]["Channel_Name"]["playlist_id"]] 

    values = df_playlist_details.values.tolist()
    cursor = conn.cursor()
 
    query = "INSERT INTO playlist (channel_id, playlist_id) VALUES (%s, %s)"
    cursor.executemany(query, values)

    conn.commit()
    
    #to migrate video_details details to video teble:
    
    df_video_details = pd.DataFrame(columns=["Video_Id", "playlist_id", "Video_Name", "Video_Description",
                                             "Published_date", "View_Count", "Like_Count",
                                             "Favorite_Count", "Comment_Count", "Duration",
                                             "Thumbnail", "Caption_Status"])
    for i in range (0, len(import_from_mongodb[0])-1):
        df_video_details.loc[i] = [import_from_mongodb[0][f"video_id_{i+1}"]["Video_Id"],
                                   import_from_mongodb[0]["Channel_Name"]["playlist_id"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Video_Name"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Video_Description"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["PublishedAt"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["View_Count"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Like_Count"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Favorite_Count"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Comment_Count"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Duration"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Thumbnail"],
                                   import_from_mongodb[0][f"video_id_{i+1}"]["Caption_Status"]]

    df_video_details["View_Count"] = pd.to_numeric(df_video_details["View_Count"])
    df_video_details["Like_Count"] = pd.to_numeric(df_video_details["Like_Count"])
    df_video_details["Favorite_Count"] = pd.to_numeric(df_video_details["Favorite_Count"])
    df_video_details["Comment_Count"] = pd.to_numeric(df_video_details["Comment_Count"])
    df_video_details['Published_date'] = pd.to_datetime(df_video_details['Published_date'], utc=True)
    
    
    def convert_duration(duration):
        time_pattern = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
        match = time_pattern.match(duration)
        if match:
            hours = int(match.group(1)[0:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[0:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[0:-1]) if match.group(3) else 0
            return f"{hours:02}:{minutes:02}:{seconds:02}"
        else:
            return None

    # Assuming 'df_video_details' is your DataFrame and 'Duration' is the column containing the duration values
    df_video_details['Duration'] = df_video_details['Duration'].astype(str)  # Convert to string format
    df_video_details['Duration'] = df_video_details['Duration'].apply(convert_duration)

    df_video_details['Duration'] = pd.to_datetime(df_video_details['Duration'])

    values = df_video_details.values.tolist()

    query = "INSERT INTO video (Video_Id, playlist_id, Video_Name, Video_Description, Published_date, View_Count, Like_Count, Favorite_Count, Comment_Count, Duration, Thumbnail, Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(query, values)

    conn.commit()
    
    ##to migrate comment_details details to comment teble:

    df_comment_details = pd.DataFrame(columns=["Comment_Id", "Video_Id", "Comment_Text",
                                               "Comment_Author", "comment_published_date"])
    all_data = []

    for i in range(len(import_from_mongodb[0]) - 1):
        video_id = import_from_mongodb[0][f"video_id_{i + 1}"]["Video_Id"]
        current_video_comments = import_from_mongodb[0][f"video_id_{i + 1}"]["Comments"]

        for j in range(len(current_video_comments)):
            comment_details_only = [
                current_video_comments[j][f"comments_id_{j + 1}"]["Comment_id"],
                video_id,
                current_video_comments[j][f"comments_id_{j + 1}"]["comment_text"],
                current_video_comments[j][f"comments_id_{j + 1}"]["comment_Author"],
                current_video_comments[j][f"comments_id_{j + 1}"]["Comment_PublishedAt"]
            ]
            all_data.append(comment_details_only)

        df_comment_details = pd.DataFrame(all_data, columns=["Comment_Id", "Video_Id", "Comment_Text",
                                                     "Comment_Author", "comment_published_date"])

    values = df_comment_details.values.tolist()

    query = "INSERT INTO comment (Comment_Id, Video_Id, Comment_Text, Comment_Author, comment_published_date) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(query, values)

    conn.commit()

    return



def create_sql_table():
        
        query="create table if not exists channel (channel_id varchar(255), channel_name varchar(255),subscription_count int, channel_views int,channel_description text);"
        cursor.execute(query)
        conn.commit()
    
        query = "create table if not exists playlist (channel_id varchar(255), playlist_id varchar(255))"
        cursor.execute(query)
        conn.commit()

        query = "create table if not exists video (Video_Id varchar(255), playlist_id varchar(255), Video_Name varchar(255), Video_Description text, Published_date date, View_Count int, Like_Count int, Favorite_Count int, Comment_Count int, Duration time, Thumbnail varchar(255), Caption_Status varchar(255))"        
        cursor.execute(query)
        conn.commit()

        query = "create table if not exists comment (Comment_Id varchar(255), Video_Id varchar(255), Comment_Text text, Comment_Author varchar(255), comment_published_date date)"
        cursor.execute(query)
        conn.commit()

        return

        
def display_output(Entire_channel_details):
    with col3:
        st.text("")
        with st.container():
            with st.expander("Channel Details in JSON Format", expanded=True):
                json_output = json.dumps(Entire_channel_details, indent=4)
                height = 300
                st.text_area("", value=json_output, height=height)
                return



#Streamlit Heading or title
                
st.markdown("<h1 style='text-align: center; color: blue; font-weight: bold; font-family: Arial;'>Youtube Data Harvesting</h1>", unsafe_allow_html=True)
st.text("")
st.text("")


col1,col2,col3 = st.columns([1,0.8,4])

with col1:
    #to get input text
    channel_id = st.text_input("Enter the channel ID", value="")

with col2:
    st.text("")
    st.text("")
    search_button = st.button("Search:mag:") 
    if search_button:
        try:
            Entire_channel_details = get_complete_channel_details(youtube, channel_id)
            display_output(Entire_channel_details)
        except KeyError:
            st.warning("Enter the valid channel id")
          
col4,col5,col6 = st.columns([1,3,1])

with col6:# to export data to Mongo DB
    st.text("")
    st.text("")
    Export_button = st.button("Export to MongoDB:rocket:")
   
    if Export_button:
        try:
            try:
                Mongodb_input1 = {'Channel_Name.Channel_id': channel_id}
                result = coll.find(Mongodb_input1, {"_id": 0, "Channel_Name.Channel_id": 1})
                x = []
                for i in result:
                    x.append(i)
                z = x[0]["Channel_Name"]["Channel_id"]

                if channel_id == z:
                    st.warning("Duplicate channel_id, Data already exists")
                else:
                    pass
            except IndexError:
                result_dict = get_complete_channel_details(youtube, channel_id)
                display_output(result_dict)
                coll.insert_many([result_dict])
                st.warning("Data successfully exported to MongoDB")
        except (KeyError,IndexError):
            st.warning("Please search the channel id before exporting to mongodb")

with col4:#to create the dropdown option
    options=[''] #initially created empty list with empty string for default blank
    #to add channel Name to dropdown options From Mongo DB
    query_result = coll.find({}, {"_id": 0, "Channel_Name.Channel_Name": 1})
    for document in query_result:
        options.append(document["Channel_Name"]["Channel_Name"])
    #Display the filtered options in the dropdown
    selected_option = st.selectbox("Select the Channel Name",options)
    import_from_mongodb=[]
    if selected_option:
            Mongodb_input={'Channel_Name.Channel_Name': selected_option}
            x= coll.find(Mongodb_input,{"_id":0})
            found_documents = False
            for i in x:
                import_from_mongodb.append(i)
                found_documents = True
            display_output(import_from_mongodb)
            if not found_documents:
                st.warning("No channel details found for the selected channel name from MongoDB.")
    
with col5: #migrating data's to sql table 
    st.text("")
    st.text("")           
    migrate_button = st.button("Migrate to SQL:rocket:")
    if migrate_button:
        try:
        
            try:
                get_SQL_table=create_sql_table()
                conn.commit()
                query = "SELECT Channel_name FROM channel WHERE Channel_name = %s;"
                cursor.execute(query, (selected_option,))
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
                if selected_option == df.channel_name[0]:
                    st.warning("Duplicate channel_id, Data already exists")
                else:
                    pass
            except IndexError: 
                migrate_to_sql= migrate_to_sql(import_from_mongodb)
                st.warning("Data successfully migrated to SQL")
        except IndexError:
            st.warning("Please select the channel name for migrating to SQL")
                  
    


questions = [
    "",
    "1.What are the names of all the videos and their corresponding channels?",
    "2.Which channels have the most number of videos, and how many videos do they have?",
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "6.What is the total number of views for each channel, and what are their corresponding channel names?",
    "7.Which videos have the highest number of comments, and what are their corresponding channel names?",
    "8.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "9.What are the names of all the channels that have published videos in the year 2022?",
    "10.What is the average duration of all videos in each channel, and what are their corresponding channel names?"
    
]

# Define the corresponding queries for each question
queries = [
    "",
    "SELECT c.video_name, A.channel_name FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id",
    "SELECT a.channel_name, COUNT(*) AS video_count FROM public.channel A  INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id GROUP BY a.channel_name ORDER BY video_count DESC LIMIT 1;",
    "SELECT A.channel_name,c.video_name, C.view_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id ORDER BY C.view_count DESC LIMIT 10",
    "SELECT c.video_name, c.comment_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id",
    "SELECT c.video_name, C.like_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id",
    "SELECT A.channel_name, SUM(C.view_count) AS total_views FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id GROUP BY A.channel_name",
    "SELECT  A.channel_name,c.video_name, c.comment_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id ORDER BY c.comment_count DESC LIMIT 1;",
    "SELECT A.channel_name,c.video_name, C.like_count FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id ORDER BY C.like_count DESC LIMIT 1;",
    "SELECT DISTINCT A.channel_name FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id WHERE EXTRACT(YEAR FROM C.published_date) = 2022;",
    "SELECT A.channel_name, TO_CHAR(AVG(C.duration::interval), 'HH24:MI:SS') AS average_duration FROM public.channel A INNER JOIN public.playlist B ON A.channel_id = B.channel_id JOIN public.video C ON B.playlist_id = C.playlist_id GROUP BY A.channel_name;"
]

# Display the dropdown to select the question
selected_question = st.selectbox("Select a question:", questions)

# Execute the corresponding query based on the selected question
query_index = questions.index(selected_question)
query = queries[query_index]
if selected_question:
    conn.commit()
    cursor.execute(query)
    results = cursor.fetchall()
    # Convert the results to a DataFrame
    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
    # Display the table in Streamlit
    st.subheader(selected_question)
    st.dataframe(df)
    conn.commit() 
    








