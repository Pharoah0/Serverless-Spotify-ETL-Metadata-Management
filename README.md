# Serverless Spotify ETL Metadata Management

Author: Pharoah Evelyn

<p align="center">
    <img src="https://github.com/Pharoah0/Serverless-Spotify-ETL-Metadata-Management/blob/main/images/serverless_spotify_etl_metadata_management.png" />
</p>

## Overview

#### This repository outlines the development of an ETL pipeline using Python and AWS services to obtain the artist, album, and song information from the "Discover Weekly" playlist on Spotify.

AWS Services used entail loading data into Amazon S3, transforming with lambda functions into a secondary S3 Bucket, then querying the transformed data using Amazon Athena, utilizing AWS Glue for cataloging and metadata management.

## Business Problem

A music enthusiast wants to understand the music industry by collecting different data to understand patterns in how Spotify recommends music based on music taste and make their music based on what's recommended or popular.

## Data Preparation

I utilized the Spotify API using the Spotipy library. A jupyter notebook was developed to test the code before importing it into Lambda functions within AWS.

## Methods Used

Extract the data from the API via Lambda function on a weekly scheduled trigger, saving it into a raw S3 Bucket zone within a `raw-data/to-be-processed` subfolder.

Transform the data within the raw zone with another lambda function, then save the transformed data into a transformation S3 Bucket zone.
The transformation code separates song, album, and artist metadata into subfolders.

Once processed, raw data in the `raw-data/to-be-processed` subfolder is moved into a `raw-data/processed` subfolder to denote that that data has been accounted for.

Once data arrives, a Glue crawler creates Glue Data Catalogs based on the transformed S3 Bucket zone. Once available in the data catalog, we can query with Athena.

## Ways to improve this project

Using different parameters, we can capture additional song details and run a deep audio analysis based on the audio features captured from the Spotify API.

Gathering this enhanced data can allow Data Scientists to explore machine-learning tactics to help predict user listening patterns.

<p align="center">
    <img src="https://github.com/Pharoah0/Serverless-Spotify-ETL-Metadata-Management/blob/main/images/0_6PV3POHQ1WxOu4rZ.jpg" />
</p>

Image for humor!
