from pymongo import MongoClient

def create_geo_database(mongo_uri, database_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]

    collections = {
        "features": {
            "schema": {
                "name": "string",
                "description": "string",
                "geometry": {
                    "projection": "string",
                    "wktLiteral": "string"
                },
                "colors": {
                    "boundaryColor": "string",
                    "fillColor": "string",
                    "pointImage": "string"
                },
                "resourceURL": "string",
                "sourceEndpoint": "ObjectId",  # Reference to endpoint ID
                "retrievalQuery": "string",
                "sourceFileType": "string",
                "data": [
                    {
                        "name": "string",
                        "value": "string",
                        "type": "string",
                        "unit": "string",
                        "description": "string",
                        "source": "string"
                    }
                ],
                "layers": ["ObjectId"],  # References to layers
                "maps": ["ObjectId"]  # References to maps
            },
            "indexes": ["name", "sourceEndpoint"]
        },
        "layers": {
            "schema": {
                "name": "string",
                "description": "string",
                "visibility": "boolean",
                "zIndex": "int",
                "source": "string",
                "resourceURL": "string",
                "features": ["ObjectId"],  # References to features
                "createdDate": "date",
                "lastUpdated": "date"
            },
            "indexes": ["name", "createdDate"]
        },
        "maps": {
            "schema": {
                "title": "string",
                "description": "string",
                "resourceURL": "string",
                "createdDate": "date",
                "lastUpdated": "date",
                "creator": "ObjectId",  # Reference to user ID
                "layers": ["ObjectId"]  # References to layers
            },
            "indexes": ["title", "creator"]
        },
        "endpoints": {
            "schema": {
                "endpointURL": "string",
                "description": "string",
                "resourceURL": "string"
            },
            "indexes": ["endpointURL"]
        },
        "users": {
            "schema": {
                "userID": "string",
                "userName": "string",
                "email": "string",
                "role": "string",
                "resourceURL": "string"
            },
            "indexes": ["userID", "email"]
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

# Example usage
mongo_uri = "mongodb://localhost:27017/"
database_name = "geo_db"
create_geo_database(mongo_uri, database_name)
