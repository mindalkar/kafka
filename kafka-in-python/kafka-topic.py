import os
import sys
import time
import getopt
from kafka.admin import KafkaAdminClient, NewTopic


def create_topic(admin_client, topic_name, partitions, replication_factor):
    topic_list = []
    res = None
    topic_list.append(NewTopic(name=topic_name, num_partitions=partitions,
                               replication_factor=replication_factor))

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        print('Unable to create topic')
        print(e)

    return res


def delete_topic(admin_client, topic_name):
    topic_list = [topic_name]
    res = None
    try:
        res = admin_client.delete_topics(topic_list)
    except Exception as e:
        print('Unable to delete topic ')
        print(e)
    return res


def list_topics(admin_client):
    details = admin_client.list_topics()
    return details


def main(argv):
    partitions = 1
    replication_factor = 1
    try:
        opts, args = getopt.getopt(argv, "ht:p:r",
                                   ["topic=", "partitions=",
                                    "replication-factor="])

    except getopt.GetoptError:
        print('kafka-topic.py --topic <topic_name> --partitions '
              '<number_of_partitions> --replication-factor '
              '<number_of_replication>')

        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('kafka-topic.py --topic <topic_name> --partitions '
                  '<number_of_partitions> --replication-factor '
                  '<number_of_replication>')
            sys.exit()

        elif opt in ("-t", "--topic"):
            topic = arg
        elif opt in ("-p", "--partitions"):
            partitions = int(arg)
        elif opt in ("-r", "--replication-factor"):
            replication_factor = int(arg)

    create_topic(topic, partitions, replication_factor)
    print 'Topic Name  ', topic
    print 'Number of partitions ', str(partitions)
    print 'Number of replications ', str(replication_factor)


if __name__ == "__main__":

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    while True:
        choice = raw_input("\nEnter choice:\n1. Create Topic\n2. List Topic\n"
                           "3. Delete Topic\n4. Exit\n")
        if choice == '1':
            topic_name = raw_input("Enter Topic Name: ")
            partitions = raw_input("Enter number of partitions"
                                   "(default=1): ").strip()
            partitions = 1 if partitions == '' else int(partitions)
            replication_factor = raw_input("Enter number of "
                                           "Replications(default=1): ").strip()
            replication_factor = (1 if replication_factor == ''
                                  else int(replication_factor))

            res = create_topic(admin_client, topic_name,
                               partitions, replication_factor)
            time.sleep(3)
            if topic_name in list_topics(admin_client):
                print('Topic got created successfully')

        elif choice == '2':
            print('Topics avaiable in kafka:-')
            for topic in list_topics(admin_client):
                print(topic)
        elif choice == '3':
            topic_name = raw_input("Enter Topic Name to be deleted: ")
            res = delete_topic(admin_client, topic_name)
            time.sleep(3)
            if topic_name not in list_topics(admin_client):
                print('Topic got deleted successfully')
        elif choice == '4':
            print('Thanks You!!!!')
            break
    # main(sys.argv[1:])
