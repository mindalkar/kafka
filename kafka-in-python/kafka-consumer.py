from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient


def list_topics(admin_client):
    details = admin_client.list_topics()
    return details


def describe_topic(admin_client, topic):
    topic_details = admin_client.describe_topics(topic)
    return topic_details


if __name__ == "__main__":

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    while True:
        choice = raw_input("\nEnter choice:\n1. List all topics\n"
                           "2. Get details of a topic\n"
                           "3. Consume messages from a topic\n4. Exit\n")
        if choice == '1':
            print('Topics avaiable in kafka:-')
            for topic in list_topics(admin_client):
                print(topic)

        elif choice == '2':
            topic_name = raw_input("Enter Topic Names to get details of: ")
            details = describe_topic(admin_client, [topic_name])
            print('Topic details:-')
            print('Name - {}'.format(details[0]['topic']))
            print('Number of partitions - '
                  '{}'.format(str(len(details[0]['partitions']))))
            print('Partition details:-')
            for partition in details[0]['partitions']:
                print(partition)
        elif choice == '3':
            topic_names = raw_input("Enter topic names from which "
                                    "want to consume messages"
                                    "(comma seperated): ")
            topic_names = topic_names.split(',')
            grp_id = raw_input("Enter group ID: ").strip()
            grp_id = None if grp_id == '' else grp_id
            msg_reading_stratergy = raw_input("Enter msg reading stratergy"
                                              "(earliest/latest) "
                                              "(default=latest): ").strip()

            msg_reading_stratergy = ('earliest' if
                                     msg_reading_stratergy == 'earliest'
                                     else 'latest')

            consumer = KafkaConsumer(
                           *topic_names,
                           bootstrap_servers=['localhost:9092'],
                           auto_offset_reset=msg_reading_stratergy,
                           enable_auto_commit=True,
                           group_id=grp_id)

            for message in consumer:
                print ("topic = {}, partition = {}: offset = {}: key = {} "
                       "value = {}" .format(message.topic,
                                            message.partition,
                                            message.offset,
                                            message.key,
                                            message.value))
        elif choice == '4':
            print('Thanks You!!!!')
            break
