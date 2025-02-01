import pandas as pd
import numpy as np
import psycopg2
from faker import Faker
import random
from sqlalchemy import create_engine

#Creating viewership data
fake = Faker()
num_users = 10000  # Adjust as needed

users = pd.DataFrame({
    'user_id': range(1, num_users + 1),
    'age': np.random.randint(18, 60, num_users),
    'gender': np.random.choice(['Male', 'Female', 'Other'], num_users, p=[0.48, 0.48, 0.04]),
    'location': [fake.city() for _ in range(num_users)],
    'device_type': np.random.choice(['Mobile', 'Smart TV', 'Laptop', 'Tablet'], num_users),
    'subscription_type': np.random.choice(['Free', 'Premium', 'Trial'], num_users, p=[0.6, 0.3, 0.1]),
    'watch_time_minutes': np.random.randint(5, 180, num_users),
    'completion_rate': np.random.uniform(10, 100, num_users),
    'content_id': np.random.randint(1, 100, num_users) 
})
#print(users)

#users.to_csv ('/Users/ninadmishra/Downloads/viewership_data.csv')

#Creating Content Metadata
categories = ['Gaming','Tech','Vlogs','Educational','Fitness','Food','Comedy','Trailers','unboxing']
languages = ['English','Hindi','Spanish','French','Japanese','German']
visibility = ['Public','Private','Unlisted']

def generate_video_id():
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=15))

num_videos = 10000
videos = []

for _ in range(num_videos):
    video_data = {
        'video_id': generate_video_id(),
        'title': fake.sentence(nb_words=6),
        'tags': ', '.join(fake.words(nb=5)),
        'category': random.choice(categories),
        'language': random.choice(languages),
        'duration': random.randint(30, 7200),  # 30 seconds to 2 hours
        'upload_date': fake.date_between(start_date='-5y', end_date='today'),
        'visibility': random.choice(visibility),
        
        'channel_id': fake.uuid4(),
        'channel_name': fake.company(),
        'subscribers': random.randint(1000, 5000000),
        'total_videos': random.randint(10, 2000),
        
        'views': random.randint(100, 50000000),
        'likes': random.randint(10, 5000000),
        'dislikes': random.randint(0, 1000000),
        'comments': random.randint(0, 50000),
        'shares': random.randint(0, 1000000),
        'watch_time': round(random.uniform(1000, 10000000), 2)
    }
    videos.append(video_data)

# Create DataFrame
df_videos = pd.DataFrame(videos)

##df_videos.to_csv ('/Users/ninadmishra/Downloads/video_metadata.csv')

#Creating ad performance data
num_ads = 10000

ads = pd.DataFrame({
    'ad_id': range(1, num_ads + 1),
    'user_id': np.random.randint(1, num_users + 1, num_ads),
    'content_id': np.random.randint(1, 100, num_ads),
    'ad_type': np.random.choice(['Skippable', 'Non-Skippable', 'Display'], num_ads),
    'ad_watch_time': np.random.randint(1, 30, num_ads),
    'ad_click': np.random.choice([True, False], num_ads, p=[0.05, 0.95]),
    'ad_revenue': np.random.uniform(0.01, 5.00, num_ads)
})

#print(ads)
#ads.to_csv('/Users/ninadmishra/Downloads/ad_metadata.csv')

subscriptions = users[['user_id', 'subscription_type']].copy()
subscriptions['signup_date'] = pd.to_datetime([fake.date_this_year() for _ in range(num_users)])
subscriptions['last_active_date'] = subscriptions['signup_date'] + pd.to_timedelta(np.random.randint(0, 365, num_users), unit='D')
subscriptions['churned'] = subscriptions['last_active_date'] < '2024-06-01'

#print(subscriptions)
#subscriptions.to_csv('/Users/ninadmishra/Downloads/subs_metadata.csv')

# PostgreSQL Database Credentials
DB_NAME = "ninadmishra"
DB_USER = "ninadmishra"
DB_PASSWORD = "THDC1982"
DB_HOST = "localhost"  # Change if hosted remotely
DB_PORT = "5432"  # Default PostgreSQL port

# Create a database connection using SQLAlchemy
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

users.to_sql('viewership_metadata',engine, if_exists='replace',index=False)
df_videos.to_sql ('content_metadata',engine, if_exists='replace',index=False)
ads.to_sql('advertisement_performance',engine, if_exists='replace',index=False)
subscriptions.to_sql('subscription_data',engine, if_exists='replace',index=False)
