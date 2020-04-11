from json import dumps

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient

from twitter_client import TwitterClient


def describe_topic(admin_client, topic):
    topic_details = admin_client.describe_topics(topic)
    return topic_details


def on_send_success(record_metadata):
    print('\nMessage sent successfully, here are details-')
    print('Topic name - {}'.format(record_metadata.topic))
    print('Partition to which msg is send '
          '- {}'.format(record_metadata.partition))
    print('Offset to which msg is sent - {}'.format(record_metadata.offset))


def on_send_error(excp):
    print('Message sending failed reason = {}'.format(str(excp)))


def produce_msg(producer, topic, key, msg, partition):
    # produce asynchronously with callbacks
    (producer.send(topic, value=msg, key=key, partition=partition)
     .add_callback(on_send_success).add_errback(on_send_error))

    # block until all async messages are sent
    producer.flush()


if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

    print('\nTo connect to your twitter developer account, '
          'pls provide below details:-\n')

    consumer_key = raw_input("Enter consumer key: ").strip()
    consumer_secret = raw_input("Enter consumer secret: ").strip()
    access_token = raw_input("Enter Access token: ").strip()
    access_token_secret = raw_input("Enter Access token secret: ").strip()

    twitter_client = TwitterClient(consumer_key, consumer_secret,
                                   access_token, access_token_secret)

    while True:
        choice = raw_input("\nEnter choice:\n"
                           "1. Get twitter_topic topic details\n"
                           "2. Send message to twitter_topic\n3. Exit\n")
        if choice == '1':
            topic_name = 'twitter_topic'
            details = describe_topic(admin_client, [topic_name])
            print('Topic details:-')
            print('Name - {}'.format(details[0]['topic']))
            print('Number of partitions - '
                  '{}'.format(str(len(details[0]['partitions']))))
            print('Partition details:-')
            for partition in details[0]['partitions']:
                print(partition)
        elif choice == '2':
            topic_name = 'twitter_topic'
            key = None
            partition = None

            print("\n\nTo get twitter msg related to a subject "
                  "which you want to feed into kafka, "
                  "pls enter below details:-\n")
            query = raw_input("Enter qurey, related to which you "
                              "want to retrieve twitter msg: ").strip()
            date_since = raw_input("Date since you want to fetch msg(format="
                                   "yyyy-mm-dd) "
                                   "(default=2020-04-01): ").strip()
            date_since = '2020-04-01' if date_since == '' else date_since
            msg_count = raw_input("Enter number of msg which "
                                  "you want to retrieve: ").strip()

            twitter_feeds = twitter_client.get_twitter_msg(query, date_since,
                                                           msg_count)

            for msg in twitter_feeds:
                res = produce_msg(producer, topic_name,
                                  key, str(msg), partition)
        elif choice == '3':
            print('Thanks You!!!!')
            break
