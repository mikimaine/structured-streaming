# Project 3

## pre requirement
- make sure you have docker and docker compose installed on your machine and Python 3.11.6 (not 3.12)

## steps to run the project

- setup python env and source it `python -m venv .venv` and `source .venv/bin/activate`
- install dependency `pip install -r requirements.txt`
- run `docker compose up` (have Docker Desktop running)

Open a new terminal tab

 - New terminal: lets first run the initial source streamer: `python reddit-api.py` : which will read from third party API and send it to kafka `topic1`
 - New terminal: `python -m spacy download en_core_web_sm`
 - lets now run the named entity extractor structured stream spark: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-named.py`

Now you can open Elastic hosted at `http://localhost:5601/` and setup your index and visualizer
