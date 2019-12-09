from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob

import twitter_credentials
import re

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class TwitterClient():
    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)

        self.twitter_user = twitter_user

    def get_twitter_client_api(self):
        return self.twitter_client

    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self, num_friends):
        friend_list = []
        for friend in Cursor(self.twitter_client.friends, id=self.twitter_user).items(num_friends):
            friend_list.append(friend)
        return friend_list

    def get_home_timeline_tweets(self, num_tweets):
        home_timeline_tweets = []
        for tweet in Cursor(self.twitter_client.home_timeline, id=self.twitter_user).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets


class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth


class TwitterStreamer():
    def __init__(self):
        self.twitter_authenticator = TwitterAuthenticator

    def StreamedTweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles twitter authentication and connection to twitter streaming API.
        listener = TwitterListener(fetched_tweets_filename)
        auth = self.twitter_authenticator.authenticate_twitter_app(self)
        stream = Stream(auth, listener)
        stream.filter(track=hash_tag_list)


# # # Twitter Stream Listener # # #
class TwitterListener(StreamListener):
    # This class just prints received tweets as std output
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on data: %s" % str(e))
        return True

    def on_error(self, status):
        if status == 420:
            return False
        print(status)


class TweetAnalyzer():

    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def analyze_sentiment(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))

        if analysis.sentiment.polarity > 0:
            return 1
        elif analysis.sentiment.polarity == 0:
            return 0
        else:
            return -1

    def tweets_to_data_frames(self, tweets):
        df = pd.DataFrame(data=[tweet.text for tweet in tweets], columns=['Tweets'])

        df['Length'] = np.array([len(tweet.text) for tweet in tweets])
        df['Source'] = np.array([tweet.source for tweet in tweets])
        df['Re-Tweets'] = np.array([tweet.retweet_count for tweet in tweets])
        df['date'] = np.array([tweet.created_at for tweet in tweets])
        df['Likes'] = np.array([tweet.favorite_count for tweet in tweets])

        return df


if __name__ == "__main__":
    username = input("Enter the username whose tweets you want to analyze: @")

    twitter_client = TwitterClient()
    tweet_analyzer = TweetAnalyzer()

    api = twitter_client.get_twitter_client_api()
    tweets = api.user_timeline(screen_name=username, count=200)

    ### Prints attributes of a tweet we can use for analysis
    #print(dir(tweets[0]))

    df = tweet_analyzer.tweets_to_data_frames(tweets)
    df['sentiment'] = np.array([tweet_analyzer.analyze_sentiment(tweet) for tweet in df['Tweets']])
    print(df.head(10))

    #Get average likes of tweets
    print(np.mean(df['Likes']))

    # Get average length of tweets
    print(np.max(df['Length']))

    #Time Series (Visualizing data using matplotlib as plt)
    time_likes = pd.Series(data = df['Likes'].values, index=df['date'])
    time_likes.plot(figsize=(16, 4), label = 'Likes' , legend = True, color = 'r')

    time_retweets = pd.Series(data=df['Re-Tweets'].values, index=df['date'])
    time_retweets.plot(figsize=(16, 4), label='Re-Tweets', legend=True, color='b')

    plt.show()




