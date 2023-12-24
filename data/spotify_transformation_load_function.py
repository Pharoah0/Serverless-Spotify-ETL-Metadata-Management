import boto3
from datetime import datetime
from io import StringIO
import json
import pandas as pd


def album(data):
    # Loop to create album data
    album_list = []
    for row in data['items']: # Iterate through each instance of information for each song on the playlist
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_info = {'album_id':album_id,
                       'name':album_name,
                       'release_date':album_release_date,
                       'total_tracks':album_total_tracks,
                       'url':album_url
                       }
        album_list.append(album_info)
    return album_list
    
def artist(data):
    # Loop to create artist data
    artist_list = []
    for row in data['items']: # Iterate through each instance of information for each song on the playlist
        artist_name = row['track']['artists'][0]['name']
        artist_id = row['track']['artists'][0]['id']
        artist_url = row['track']['artists'][0]['href']
        artist_info = {'name':artist_name,
                       'artist_id':artist_id,
                       'url':artist_url}
        artist_list.append(artist_info)
    return artist_list
    
def songs(data):
    # Loop to create artist data
    song_list = []
    for row in data['items']: # Iterate through each instance of information for each song on the playlist
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration_ms = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added_at = row['added_at']
        song_album_id = row['track']['album']['id']
        song_artist_id = row['track']['album']['artists'][0]['id']
        song_info = {'id':song_id,
                       'name':song_name,
                       'duration_ms':song_duration_ms,
                       'url':song_url,
                       'popularity':song_popularity,
                       'song_added':song_added_at,
                       'album_id': song_album_id,
                       'artist_id': song_artist_id
                       }
        song_list.append(song_info)
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-project-pharoah"
    Key = "raw-data/to-be-processed/"
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']: # Retrieves each file name within the S3 Bucket.
        file_key = file['Key']
        if file_key.split('.')[-1] == "json": # Ommits the .json suffix on the file name
            response = s3.get_object(Bucket = Bucket, Key = file_key) # Reads the file
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)
        
        # Loading the data frames
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)
        
        # Converting necessary columns into datetime format!
        album_df['release_date'] = pd.to_datetime(album_df['release_date'], format='ISO8601')
        song_df['song_added'] = pd.to_datetime(song_df['song_added'], format='ISO8601')
        
        # Saving data to Transformation S3 Bucket in their respective folders
        
        album_key = "transformed-data/album-data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = "transformed-data/artist-data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
        songs_key = "transformed-data/songs-data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer=StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)
        
    # Moving data from our to-be-processed folder to the processed folder
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw-data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()