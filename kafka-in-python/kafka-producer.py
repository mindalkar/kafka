from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient


def list_topics(admin_client):
    details = admin_client.list_topics()
    return details


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
    while True:
        choice = raw_input("\nEnter choice:\n1. List all topics\n"
                           "2. Get details of a topic\n"
                           "3. Send message to topic\n4. Exit\n")
        if choice == '1':
            print('Topics avaiable in kafka:-')
            for topic in list_topics(admin_client):
                print(topic)

        elif choice == '2':
            topic_name = raw_input("Enter Topic Name to get details of: ")
            details = describe_topic(admin_client, [topic_name])
            print('Topic details:-')
            print('Name - {}'.format(details[0]['topic']))
            print('Number of partitions - '
                  '{}'.format(str(len(details[0]['partitions']))))
            print('Partition details:-')
            for partition in details[0]['partitions']:
                print(partition)
        elif choice == '3':
            topic_name = raw_input("Enter Topic Name to "
                                   "which you want to send msg: ")
            msg = raw_input("Enter message: ")
            key = raw_input("Enter key for message (default=None): ").strip()
            key = None if key == '' else key
            partition = raw_input("Enter partition to which msg is to be send"
                                  "(default=round_robin): ").strip()
            partition = None if partition == '' else int(partition)

            res = produce_msg(producer, topic_name, key, msg, partition)
        elif choice == '4':
            print('Thanks You!!!!')
            break
