import dateutil.parser
import json
import time

from datetime import date, timedelta
from google.cloud import pubsub_v1
from google.cloud import secretmanager
from newsapi import NewsApiClient

topic = "new-urls"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("tldr-278619", topic)

client = secretmanager.SecretManagerServiceClient()
secret_name = client.secret_version_path("tldr-news-discovery", "newsapi_org_api_key", "1")
secret_response = client.access_secret_version(secret_name)
news_api_key = secret_response.payload.data.decode("UTF-8")

newsapi = NewsApiClient(api_key=news_api_key)


def process_function_call(event, context):
    publish_latest_news_to_pipeline()


def publish_latest_news_to_pipeline():
    yesterday = date.today() - timedelta(days=1)
    # /v2/top-headlines
    top_headlines = newsapi.get_top_headlines(category="technology",
                                              language="en",
                                              country="us")

    for headline in top_headlines["articles"]:
        headline_date = dateutil.parser.parse(headline["publishedAt"]).date()
        if yesterday < headline_date:
            print(headline["title"])
            print(headline["url"])
            url = headline["url"]
            msg_dict = {
                "url": url,
                "test": True
            }
            msg_str = json.dumps(msg_dict)
            print("msg to post: {}".format(msg_str))
            msg_data = msg_str.encode("utf-8")
            publisher.publish(
                topic_path, msg_data
            )
            print("published")
            # FIXME: On average it takes 30 seconds to process new URL
            # BERT is the bottle neck.
            time.sleep(10)


if "__main__" == __name__:
    publish_latest_news_to_pipeline()
