import os
import requests
import pymongo
import logging
import time

def get_existing_topics(collection):
    existing_topics = []
    
    topics = collection.find({})

    for topic in topics:
        existing_topics.append(topic['topic'])

    return existing_topics

def main():
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',level=logging.DEBUG)

    logging.info("Starting new topic insert for saga")

    TOPIC_FILE_URL = os.environ['TOPIC_FILE_URL']
    #TOPIC_FILE_URL = 'https://raw.githubusercontent.com/JohnZProd/saga-poc/master/doc_list.txt'
    try:
        logging.error("Getting doc list from " + TOPIC_FILE_URL)
        r = requests.get(TOPIC_FILE_URL)
        content = r.content.decode('ascii')
        topics = content.split('\n')
    except:
        logging.error("Error getting doc list from url " +TOPIC_FILE_URL)
        exit(0)

    topic_file_list = []
    topics_to_add = []
    topics_to_delete = []
    existing_topics = []

    try:
        logging.info("Connecting to DB")
        DB_USER = os.environ['DB_USER']
        DB_PASSWORD = os.environ['DB_USER_PASS']
        DB_NAME = os.environ['DB_NAME']
        DB_ENDPOINT = os.environ['DB_ENDPOINT']

        client = pymongo.MongoClient("mongodb://{}:{}@{}/{}".format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_NAME))
        db = client.saga
        collection = db.topics
    except:
        logging.error("Could not connect to db")
        exit(0)

    existing_topics = get_existing_topics(collection)
    
    for topic_row in topics:
        try:
            topic_data = topic_row.split(',')
            topic = topic_data[0]
            filtered_url = topic_data[1]
            category = topic_data[2]
        
            # For delete logic 
            topic_file_list.append(topic)

            if topic in existing_topics:
                logging.info("Already found " + topic)
            else:
                topics_to_add.append({
                    "topic": topic,
                    "filtered-url": filtered_url,
                    "category": category
                })
        except:
            logging.warning("Failed to process row " + topic_row)

    logging.info("Deleting topics that are not in doc list")
    for existing_topic in existing_topics:
        if existing_topic not in topic_file_list:
            topics_to_delete.append(existing_topic)

    if len(topics_to_add) > 0:
        logging.info("Adding the following entries into db:")
        for topic in topics_to_add:
            logging.info(topic['topic'])
        x = collection.insert_many(topics_to_add)
    else:
        logging.info("Nothing to add")

    if len(topics_to_delete) > 0:
        logging.info("Deleting the following entries from db:")
        for topic in topics_to_delete:
            logging.info(topic)
        x = collection.delete_many({"topic": {"$in": topics_to_delete}})
    else:
        logging.info("Nothing to delete")
    
    logging.info("All processes complete")

if __name__ == "__main__":
    main()
