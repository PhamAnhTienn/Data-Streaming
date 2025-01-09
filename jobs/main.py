import os
import uuid
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from groq import Groq
import simplejson as json
import random
import requests
import googleapiclient.discovery
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

load_dotenv()

# Retrieve the API key
DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")
GROK_API_KEY = os.getenv("GROK_API_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = DEVELOPER_KEY)
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=CLIENT_ID,  
    client_secret=CLIENT_SECRET
))

keywords = ["coding", "music", "travel", "funny", "technology"]

#Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
YOUTUBE_TOPIC = os.environ.get('YOUTUBE_TOPIC', 'youtube_data')
MUSIC_TOPIC = os.environ.get('MUSIC_TOPIC', 'music_data')
WEATHER_TOPIC = os.environ.get('WEATHER_TOPIC', 'weather_data')

def generate_youtube_data():
    random_keyword = random.choice(keywords)

    search_request = youtube.search().list(
        part="snippet",
        q=random_keyword,
        type="video",
        maxResults=5  
    )

    search_response = search_request.execute()

    videos = search_response['items']
    random_video = random.choice(videos)

    video_id = random_video['id']['videoId']
    video_details_request = youtube.videos().list(
        part="snippet,statistics,status,topicDetails,recordingDetails",
        id=video_id
    )

    video_details_response = video_details_request.execute()
    video_info = video_details_response['items'][0]

    channel_id = video_info['snippet']['channelId']
    channel_request = youtube.channels().list(
        part="snippet,statistics,topicDetails,brandingSettings",
        id=channel_id
    )
    channel_response = channel_request.execute()
    channel_info = channel_response['items'][0]
    
    video_data = {
        "id": video_info['id'],
        "title": video_info['snippet']['title'],
        "published_date": video_info['snippet']['publishedAt'].split('T')[0],
        "channel_id": video_info['snippet']['channelId'],
        "channel_title": video_info['snippet']['channelTitle'],
        "view_count": int(video_info['statistics'].get('viewCount', 0)),
        "like_count": int(video_info['statistics'].get('likeCount', 0)),
        "comment_count": int(video_info['statistics'].get('commentCount', 0)),
        "category_id": video_info['snippet']['categoryId'],
        "topic_categories": [topic.split('/')[-1] for topic in video_info['topicDetails'].get('topicCategories', [])],
        "country": channel_info['snippet'].get('country', 'Unknown')
    }
    
    return video_data

def create_prompt(video_data):
    topic_categories = ", ".join(video_data['topic_categories']).replace("_", " ")
    country = video_data['country']

    prompt = f"""
You are an AI model. Based on the following video data, your task is to return one city in the given country and a keyword related to music.
This keyword should be inspired by the video's topic categories.
The keyword will be used to search for songs on Spotify.
Ensure that the keyword is a single word in English, for example: "love", "dance", "jazz".

Here is the country and topic in the video data:
- Country: {country}
- Topic Categories: {topic_categories}

Only return the city and the keyword. Do not include any additional information.
"""
    return prompt

def get_completion(video_data):
    prompt = create_prompt(video_data)

    client = Groq(api_key=GROK_API_KEY)

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model="llama-3.3-70b-versatile",
    )

    response = chat_completion.choices[0].message.content.strip()
    city, keyword = response.split(',')
    return city, keyword

def generate_music_data(keyword, limit=1):
    results = sp.search(q=keyword, type='track', limit=limit)
    item = results['tracks']['items'][0]
    
    track = sp.track(item['id'])
    track_info = {
        "id": item['id'],
        "Name": item['name'],
        "Artist": ", ".join(artist['name'] for artist in item['artists']),
        "Album": track['album']['name'],
        "Release Date": track['album']['release_date'],
        "Genres": track['album'].get('genres', []),
        "Popularity": track['popularity'],
        "Duration (minutes)": round(track['duration_ms'] / 60000, 2),
        "Track URL": item['external_urls']['spotify'],
        "Available Markets": len(track['available_markets'])
    }
    
    return track_info
    
def generate_weather_data(city):
    weather_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
    weather_response = requests.get(weather_url)
    weather_data = weather_response.json()
    
    weather_id = weather_data.get("id", "N/A")
    city_name = weather_data.get("name", "Unknown")
    country = weather_data["sys"].get("country", "Unknown")
    temperature = weather_data["main"].get("temp", "N/A")
    feels_like = weather_data["main"].get("feels_like", "N/A")
    description = weather_data["weather"][0].get("description", "No description").capitalize()
    humidity = weather_data["main"].get("humidity", "N/A")
    wind_speed = weather_data["wind"].get("speed", "N/A")
   
    result = {
        "id": weather_id,
        "City": city_name,
        "Country": country,
        "Temperature (°C)": temperature,
        "Feels Like (°C)": feels_like,
        "Weather Description": description,
        "Humidity (%)": humidity,
        "Wind Speed (m/s)": wind_speed
    }
    
    return result

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic=topic, 
        key=str(data['id']), 
        value=json.dumps(data, default=json_serializer).encode('utf-8'), 
        on_delivery=delivery_report
    )
    
    producer.flush()

def generate_data():
    while True:
        youtube_data = generate_youtube_data()
        city, keyword = get_completion(youtube_data)
        music_data = generate_music_data(keyword)
        weather_data = generate_weather_data(city)
        
        produce_data_to_kafka(producer, YOUTUBE_TOPIC, youtube_data)
        produce_data_to_kafka(producer, MUSIC_TOPIC, music_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        
        break
        
if __name__ == '__main__':
    # Create a Serializing Producer
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Error: {err}')
    }
    
    producer = SerializingProducer(producer_config)
    
    try:
        generate_data()
    except KeyboardInterrupt:
        print('Keyboard Interrupt and ended by user')
    except Exception as e:
        print(f'An exception occurred: {e}')
