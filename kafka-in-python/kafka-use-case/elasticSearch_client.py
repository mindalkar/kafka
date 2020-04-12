import json
from elasticsearch import Elasticsearch
"""
 do - pip install elasticsearch
 configure elastic search DB using:-
    https://www.thegeekstuff.com/2019/04/install-elasticsearch/
"""


class ElasticSearchClient:
    def __init__(self, host, scheme="http", port="9200"):

        self.es_client = Elasticsearch(
                         [host],
                         scheme=scheme,
                         port=port,
        )

    def put_data_in_es(self, index, data, doc_type=None, id=None):

        result = self.es_client.index(index=index, body=data,
                                      id=id, doc_type=doc_type)

        return result


if __name__ == "__main__":

    es_host = raw_input("Enter ElasticSearch host: ").strip()

    es_index = raw_input("\nEnter ElasticSearch index: ").strip()

    json_data = raw_input("Enter data to put in "
                          "ElasticSearch (in JSON format)").strip()
    json_data = json.loads(json_data)

    doc_type = raw_input("Enter ES data doc type: ").strip()
    doc_type = None if doc_type == '' else doc_type

    data_id = raw_input("Enter ES data ID: ").strip()
    data_id = None if data_id == '' else data_id

    es_client = ElasticSearchClient(es_host)

    res = es_client.put_data_in_es(es_index, json_data, doc_type, data_id)

    print(res)
