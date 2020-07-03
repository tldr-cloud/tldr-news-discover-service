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
    publish_top_headlights_to_pipeline()
    publish_latest_news_to_pipeline()


def publish_url(url):
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


def publish_latest_news_to_pipeline():
    sources = newsapi.get_sources(category="technology", language="en", country="us")
    # print(type(sources))
    # print(sources)
    sources = [source["id"] for source in sources["sources"]]
    sources_str = ",".join(sources)
    print("sources: {}".format(sources_str))
    yesterday = date.today() - timedelta(days=1)
    top_headlines = newsapi.get_everything(from_param=yesterday,
                                           language="en",
                                           sort_by="relevancy",
                                           sources=sources_str,
                                           page_size=50)
    for headline in top_headlines["articles"]:
        print(headline["title"])
        print(headline["url"])
        publish_url(headline["url"])


def publish_top_headlights_to_pipeline():
    yesterday = date.today() - timedelta(days=1)
    # /v2/top-headlines
    top_headlines = newsapi.get_top_headlines(category="technology",
                                              language="en",
                                              country="us",
                                              page_size=100)
    print(str(top_headlines))
    for headline in top_headlines["articles"]:
        headline_date = dateutil.parser.parse(headline["publishedAt"]).date()
        if yesterday < headline_date:
            print(headline["title"])
            print(headline["url"])
            publish_url(headline["url"])


if "__main__" == __name__:
    publish_latest_news_to_pipeline()
    publish_top_headlights_to_pipeline()
