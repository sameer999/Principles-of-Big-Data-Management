from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

consumer_key="Azv2WADinSuE49URwZEG7fYor"
consumer_secret="uLQkVZHhFkp9CGWQVlNYJEbbOWTG68KuPcmGDTFjMT3929OBjI"
access_token="3325902110-zAKeVetGeyl1q1RIWNDuPMImY1WMtS7KUScZLaY"
access_token_secret="nIgdhWY8n0oFUHTsy6Icxu0K2Z93dcold8c4Q7AL7Wj7p"

class StdOutListener(StreamListener):
    def on_data(self, data):
        try:
            with open('data10.json', 'a') as outfile:
                json.dump(data,outfile)
            with open('data20.json','a') as outputj:
                outputj.write(data)
            with open('tweetsdata.txt', 'a') as tweets:
                tweets.write(data)
                tweets.write('\n')
            outfile.close()
            tweets.close()
            outputj.close()
        except BaseException as e:
            print('problem collecting tweet',str(e))
        return True
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['icc','kohli','insta','crypto','happy','win','bitcoin','analytics','python','smart','science','data','apple','iphone','tesla',])
