from pymongo import MongoClient

def create_geo_database(mongo_uri, db_name):
    # Establish connection to MongoDB server
    client = MongoClient(mongo_uri)
    database = client[db_name]

    # List of collections to be created in the database
    required_collections = [
        "geo_features",
        "geo_layers",
        "geo_maps",
        "geo_sparqlendpoints",
        "geo_users",
        "geo_data"
    ]

    # Create collections if they do not exist
    for collection in required_collections:
        if collection not in database.list_collection_names():
            database.create_collection(collection)
            print(f"Collection '{collection}' created.")

    print(f"Database '{db_name}' setup with collections successfully.")

# ExampleL
mongo_uri = "mongodb://localhost:27017/"
db_name = "geo_db"

create_geo_database(mongo_uri, db_name)
