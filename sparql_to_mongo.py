import requests
from pymongo import MongoClient
from datetime import datetime

def send_sparql_request(sparql_query, sparql_endpoint):
    params = {
        'query': sparql_query,
        'format': 'application/sparql-results+json'
    }
    response = requests.get(sparql_endpoint, params=params)
    
    if response.status_code != 200:
        raise Exception(f"SPARQL query failed with status code {response.status_code}")
    
    return response.json()

def parse_sparql_results(results, schema_mapping):
    documents = []
    for result in results['results']['bindings']:
        document = {field: None for field in schema_mapping.values()}  
        for key, value in result.items():
            # Use schema_mapping to find the correct field in the schema
            if key in schema_mapping:
                field = schema_mapping[key]
                if value['type'] == 'uri':
                    document[field] = value['value']
                elif value['type'] == 'literal':
                    document[field] = value['value']
                    if 'datatype' in value:
                        document[field + '_type'] = value['datatype']
                elif value['type'] == 'bnode':
                    document[field] = value['value']
        
        if 'createdDate' not in document or document['createdDate'] is None:
            document['createdDate'] = datetime.utcnow()
        if 'lastUpdated' not in document or document['lastUpdated'] is None:
            document['lastUpdated'] = datetime.utcnow()
        
        documents.append(document)
    
    return documents

def store_documents_to_mongo(documents, mongo_uri, database_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    if documents:
        collection.insert_many(documents)
        print(f"Inserted {len(documents)} documents into MongoDB collection '{collection_name}'")
    else:
        print("No documents to insert")

def store_sparql_results_to_mongo(sparql_query, sparql_endpoint, mongo_uri, database_name, collection_name):

    results = send_sparql_request(sparql_query, sparql_endpoint)
    
    schema_mapping = {
        "subject": "name",
        "predicate": "description",
        "object": "coordinates"
        # We can add more here
    }
    
    documents = parse_sparql_results(results, schema_mapping)
    
    store_documents_to_mongo(documents, mongo_uri, database_name, collection_name)

def test_store_sparql_results_to_mongo():
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

# Test function
test_store_sparql_results_to_mongo()