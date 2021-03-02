import pandas as pd
import itertools
import asyncio
import typing
import json
import argparse
from elasticsearch import Elasticsearch

ES = Elasticsearch(
    "http://betaweb002.bw.webis.de:9200",
    sniff_on_start=True,
    timeout=60
)

FIELDS = [
    "body_lang.en",
    "title_lang.en",
    "meta_desc_lang.en"
]

WEIGHTS = [0, 0.2, 0.4, 0.6, 0.8, 1]


def filter_for_id(document_id):
    if "clueweb" in document_id:
        return {"term": {"warc_trec_id": document_id}}
    elif 'urn' in document_id:
        return {"term": {"warc_record_id": document_id}}
    else:
        raise


async def query_handler(index: list, topic_num: int, query: str, fields: list, trec_id: str) -> dict:
    """
    Async handler that executes an ES query with the specified parameters
    :param index: indices to perform the search in
    :param topic_num: number of the search topics
    :param query: query string to execute search with
    :param fields: fields to match query string against
    :param trec_id: TREC Id of the desired document
    :return: dict with aggregated search result
    """
    req = ES.search(
        index=index,
        body={
            "query": {
                "bool": {
                    "must": {
                        "simple_query_string": {
                            "query": query,
                            "fields": fields,
                            "default_operator": "and"
                        },
                    },
                    "filter": filter_for_id(trec_id),
                },
            },
        },
        search_type="dfs_query_then_fetch",
    )
    # If document is not found, default score 0
    if req["hits"]["total"] == 0:
        return {
            "qid": topic_num,
            "doc": trec_id,
            "rank": 0,
            "score": 0
        }
    # If document is found, return ES score
    elif (req["hits"]["total"] == 1) and \
            (
                ("warc_trec_id" in req["hits"]["hits"][0]["_source"] and req["hits"]["hits"][0]["_source"]["warc_trec_id"] == trec_id)
                or
                ("warc_record_id" in req["hits"]["hits"][0]["_source"] and req["hits"]["hits"][0]["_source"]["warc_record_id"] == trec_id)
            ):
        return {
            "qid": topic_num,
            "doc": trec_id,
            "rank": 0,
            "score": req["hits"]["hits"][0]["_score"]
        }
    # If all else fails, raise
    else:
        raise


def topic_handler(data: dict, fields: dict) -> typing.Tuple[typing.Dict]:
    """
    Handler function to async execute search and aggregate all search request for one topic
    :param fields: weighting for fields to search on
    :param data: input data dict
    :return:
    """
    args = [(
        doc["index"],
        data["topicNumber"],
        data["query"],
        fields,
        doc["trecId"]
    ) for doc in data['documentsToScore']]

    loop = asyncio.get_event_loop()
    tasks = itertools.starmap(query_handler, args)
    output = loop.run_until_complete(asyncio.gather(*tasks))
    return output


def create_fields(fields, weights) -> list:
    l = [list(zip(fields, x)) for x in itertools.permutations(weights, len(fields))]
    return [["^".join([str(z) for z in y]) for y in x] for x in l]

def parse_args():
    parser = argparse.ArgumentParser(description="TBD :)")
    parser.add_argument("--inputJson", type=str, required=True)
    parser.add_argument("--outputDir", type=str, required=True)

    return parser.parse_args()

def main():
    args = parse_args()
    print("Connected to ElasticSearch, server version {}".format(ES.info()["version"]["number"]))
    fields = [
        ["body_lang.en^1", "title_lang.en^0", "meta_desc_lang.en^0"],
        ["body_lang.en^0", "title_lang.en^1", "meta_desc_lang.en^0"],
        ["body_lang.en^0", "title_lang.en^0", "meta_desc_lang.en^1"]
    ]
    for weights in fields:
        with open(args.inputJson) as jsonl_file:
            topics = []
            for line in jsonl_file.readlines():
                data = json.loads(line)
                if len(data["documentsToScore"]) <= 0:
                    continue

                tag = "{}-{}".format(data["documentsToScore"][0]["index"], '-'.join(weights))
                print("{}, Topic {}".format(tag, data["topicNumber"]))
                df_topic = pd.DataFrame(topic_handler(data, weights))
                df_topic["Q0"] = "Q0"
                df_topic["tag"] = tag
                df_topic = df_topic.loc[:, ["qid", "Q0", "doc", "rank", "score", "tag"]]
                topics.append(df_topic)
            pd.concat(topics).to_csv(args.outputDir + "/" +tag, sep=" ", header=False, index=False)


if __name__ == '__main__':
    main()
