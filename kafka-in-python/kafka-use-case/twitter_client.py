import twitter

"""
 do - pip install python-twitter
"""


class TwitterClient:
    def __init__(self, consumer_key, consumer_secret, access_token,
                 access_token_secret):

        self.api = twitter.Api(consumer_key=consumer_key,
                               consumer_secret=consumer_secret,
                               access_token_key=access_token,
                               access_token_secret=access_token_secret)

    def get_twitter_msg(self, query, since, count):

        raw_query = ("q={}%20&result_type=recent&"
                     "since={}&count={}". format(query, since, count))

        result = self.api.GetSearch(raw_query=raw_query)

        return result


if __name__ == "__main__":

    consumer_key = raw_input("Enter consumer key: ").strip()
    consumer_secret = raw_input("Enter consumer secret: ").strip()
    access_token = raw_input("Enter Access token: ").strip()
    access_token_secret = raw_input("Enter Access token secret: ").strip()

    query = raw_input("Enter qurey, related to which you "
                      "want to retrieve twitter msg: ").strip()
    date_since = raw_input("Date since you want to fetch msg(format="
                           "yyyy-mm-dd) "
                           "(default=2020-04-01): ").strip()
    date_since = '2020-04-01' if date_since == '' else date_since
    msg_count = raw_input("Enter number of msg which "
                          "you want to retrieve: ").strip()

    twitter_client = TwitterClient(consumer_key, consumer_secret,
                                   access_token, access_token_secret)

    query_results = twitter_client.get_twitter_msg(query,
                                                   date_since, msg_count)
    for msg in query_results:
        print(msg.text)
