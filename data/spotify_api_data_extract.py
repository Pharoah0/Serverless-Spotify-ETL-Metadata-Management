import boto3
from datetime import datetime
import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    # Extract the URI from a selected playlist
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXcVIOxMHbkW8a?si=74bba8531ff8453b"
    playlist_uri = playlist_link.split("/")[-1].split('?')[0]
    
    # Return JSON Information from playlist
    data = sp.playlist_tracks(playlist_uri)
    
    # Loading data into our Raw S3 Bucket
    client = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket="spotify-etl-project-pharoah",
        Key="raw-data/to-be-processed/" + filename,
        Body=json.dumps(data)
        )
    
