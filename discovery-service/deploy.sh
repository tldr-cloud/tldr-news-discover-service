#!/bin/bash

gcloud functions deploy discovery-service  \
    --region us-central1 \
    --project tldr-news-discovery \
    --entry-point process_function_call \
    --runtime python38 \
    --memory 256 \
    --trigger-topic "daily-news-scan" \
    --service-account tldr-news-discoverer@tldr-news-discovery.iam.gserviceaccount.com