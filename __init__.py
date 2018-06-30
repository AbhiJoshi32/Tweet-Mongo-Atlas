from db import db
from time import sleep
from random import randint
from api.twitter_api import TwitterApiImpl
from config import ID_LIST

twitter_api_impl = TwitterApiImpl()

col = 'tweets'

tweet_db = db('tweets')


# noinspection PyProtectedMember
class TweetListener(twitter_api_impl.stream_listener):
    def __init__(self):
        self.back_off = 1

    def on_data(self, data):
        print(data)
        self.back_off = 1
        doc = {'tweet': data}
        tweet_db.insert_doc(doc, col)
        return True

    def on_error(self, status_code):
        print('error status code' + str(status_code))
        if status_code == 420:
            sleep((2 ** self.back_off) + (randint(0, 1000) / 1000))
            self.back_off = self.back_off + 1
            return True

    def on_timeout(self):
        print('timeout')
        sleep((2 ** self.back_off) + (randint(0, 1000) / 1000))
        self.back_off = self.back_off + 1
        return False


if __name__ == '__main__':
    tweet_listener = TweetListener()
    twitter_stream = twitter_api_impl.create_twitter_stream(tweet_listener)
    twitter_stream.filter(follow=ID_LIST)
