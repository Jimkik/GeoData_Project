import requests
from pymongo import MongoClient

def store_sparql_results_to_mongo(sparql_query, sparql_endpoint, mongo_uri, database_name, collection_name):
    #Send the SPARQL query to the SPARQL endpoint
    params = {
        'query': sparql_query,
        'format': 'application/sparql-results+json'
    }
    response = requests.get(sparql_endpoint, params=params)
    
    if response.status_code != 200:
        raise Exception(f"SPARQL query failed with status code {response.status_code}")
    
    results = response.json()
    
    # Connection to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    # Process the SPARQL results and store them in MongoDB
    for result in results['results']['bindings']:
        document = {}
        for key, value in result.items():
            #Store the values
            if value['type'] == 'uri':
                document[key] = value['value']
            elif value['type'] == 'literal':
                document[key] = value['value']
                if 'datatype' in value:
                    document[key + '_type'] = value['datatype']
            elif value['type'] == 'bnode':
                document[key] = value['value']
        
        # Insert the document into the MongoDB collection
        collection.insert_one(document)
    
    print(f"Inserted {len(results['results']['bindings'])} documents into MongoDB collection '{collection_name}'")

# Example for 10 documents:
sparql_query = """
SELECT ?subject ?predicate ?object
WHERE {
  ?subject ?predicate ?object
} LIMIT 10
"""
sparql_endpoint = "https://dbpedia.org/sparql"
mongo_uri = "mongodb://localhost:27017/"
database_name = "geo_db"
collection_name = "geo_features"

store_sparql_results_to_mongo(sparql_query, sparql_endpoint, mongo_uri, database_name, collection_name)
