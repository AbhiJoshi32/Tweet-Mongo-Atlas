import tweepy
from config import *


class TwitterApiImpl:
    def __init__(self):
        # Consumer keys and access tokens, used for OAuth
        # OAuth process, using the keys and tokens
        self.auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        self.auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        # Creation of the actual interface, using authentication
        self.twitter_api = tweepy.API(self.auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.stream_listener = tweepy.StreamListener
        self.twitter_stream = None

    def create_twitter_stream(self, listener):
        self.twitter_stream = tweepy.Stream(self.auth, listener)
        return self.twitter_stream
