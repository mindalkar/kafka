"""
This script consumes the kafka msgs and puts it in
elasticSearch database.
"""
import json

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from elasticSearch_client import ElasticSearchClient


def describe_topic(admin_client, topic):
    topic_details = admin_client.describe_topics(topic)
    return topic_details


if __name__ == "__main__":

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    print("Enter ElasticSearch details below:-\n")
    es_host = raw_input("Enter ElasticSearch host: ").strip()
    es_index = raw_input("Enter ElasticSearch "
                         "index(default=twitter): ").strip()
    es_index = 'twitter' if es_index == '' else es_index

    doc_type = raw_input("Enter ES data doc type(default=tweets): ").strip()
    doc_type = 'tweets' if doc_type == '' else doc_type

    es_client = ElasticSearchClient(es_host)
    topic_names = ['twitter_topic']
    consumer = KafkaConsumer(
                           *topic_names,
                           bootstrap_servers=['localhost:9092'],
                           auto_offset_reset='earliest',
                           group_id='twitter-grp')

    while True:
        choice = raw_input("\nEnter choice:"
                           "\n1. Get details of topic twitter_topic\n"
                           "2. Consume messages from twitter_topic topic and "
                           "put it into elasticSearch\n3. Exit\n")

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
            while True:
                msg_pack = consumer.poll(timeout_ms=100)
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        tweet = json.loads(msg.value)
                        data_id = tweet['id']
                        res = es_client.put_data_in_es(es_index, tweet,
                                                       doc_type, data_id)
                        print(res)
        elif choice == '3':
            print('Thanks You!!!!')
            break
