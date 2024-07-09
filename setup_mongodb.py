from pymongo import MongoClient

def create_geo_database(mongo_uri, database_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]

    collections = {
        "geo_features": {
            "schema": {
                "name": "string",
                "description": "string",
                "coordinates": "string",
                "projection": "string",
                "boundaryColor": "string",
                "fillColor": "string",
                "pointImage": "string",
                "sourceEndpoint": "string",
                "retrievalQuery": "string",
                "sourceFileType": "string",
                "createdDate": "date",
                "lastUpdated": "date",
                "data": [
                    {
                        "name": "string",
                        "value": "string",
                        "type": "string",
                        "unit": "string",
                        "description": "string",
                        "source": "string",
                        "lastUpdated": "date"
                    }
                ],
                "layers": [
                    {
                        "name": "string",
                        "description": "string",
                        "visibility": "boolean",
                        "zIndex": "int",
                        "source": "string",
                        "features": ["ObjectId"]
                    }
                ],
                "maps": [
                    {
                        "title": "string",
                        "description": "string",
                        "createdDate": "date",
                        "lastUpdated": "date",
                        "layers": ["ObjectId"]
                    }
                ]
            },
            "indexes": ["name", "createdDate"]
        },
        "geo_sparqlendpoints": {
            "schema": {
                "endpointURL": "string",
                "description": "string"
            },
            "indexes": ["endpointURL"]
        },
        "geo_users": {
            "schema": {
                "userID": "string",
                "userName": "string",
                "email": "string",
                "role": "string"
            },
            "indexes": ["userID"]
        }
    }
    
    for collection_name, config in collections.items():
        if collection_name not in db.list_collection_names():
            collection = db.create_collection(collection_name)
            for index in config["indexes"]:
                collection.create_index(index)
            print(f"Created collection: {collection_name} with indexes {config['indexes']}")
        else:
            print(f"Collection {collection_name} already exists")

    print(f"Database '{database_name}' and collections created successfully.")

# Example
mongo_uri = "mongodb://localhost:27017/"
database_name = "geo_db"

create_geo_database(mongo_uri, database_name)
