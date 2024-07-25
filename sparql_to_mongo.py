import requests
from pymongo import MongoClient
from rdflib import Graph, Namespace
import geojson
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
        document = {field: None for field in schema_mapping.values()}  # Initialize with schema mapping values
        for key, value in result.items():
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
        
        # Additional fields for geo_features
        if 'createdDate' not in document or document['createdDate'] is None:
            document['createdDate'] = datetime.utcnow()
        if 'lastUpdated' not in document or document['lastUpdated'] is None:
            document['lastUpdated'] = datetime.utcnow()
        
        documents.append(document)
    
    return documents

def parse_geosparql_results(sparql_query, sparql_endpoint, schema_mapping):
    sparql_results = send_sparql_request(sparql_query, sparql_endpoint)
    print("SPARQL Results: ", sparql_results)  # Έλεγχος των αποτελεσμάτων του SPARQL query
    documents = parse_sparql_results(sparql_results, schema_mapping)
    
    has_geometry = False
    for document in documents:
        if 'geometry' in document and document['geometry']:
            has_geometry = True
            wkt_literal = document['geometry']
            geojson_geometry = wkt_to_geojson(wkt_literal)
            document['geometry'] = geojson_geometry
    
    return documents, has_geometry

def wkt_to_geojson(wkt_literal):
    g = Graph()
    wkt = Namespace("http://www.opengis.net/ont/geosparql#")
    geom = g.parse(data=f"<http://example.org/geom> {wkt.asWKT} '{wkt_literal}'^^{wkt.wktLiteral} .", format="turtle")
    for s, p, o in geom:
        if p == wkt.asWKT:
            geojson_geometry = geojson.loads(o)
            return geojson_geometry
    return None

def store_documents_to_mongo(documents, mongo_uri, database_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    if documents:
        collection.insert_many(documents)
        print(f"Inserted {len(documents)} documents into MongoDB collection '{collection_name}'")
    else:
        print("No documents to insert")

def store_sparql_endpoint(sparql_endpoint, mongo_uri, database_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    document = {
        "endpointURL": sparql_endpoint,
        "description": "SPARQL endpoint with geometry data",
        "resourceURL": sparql_endpoint
    }
    
    collection.insert_one(document)
    print(f"Stored SPARQL endpoint '{sparql_endpoint}' in MongoDB collection '{collection_name}'")

def test_sparql_query():
    sparql_query = """
    SELECT ?subject ?predicate ?object
    WHERE {
      ?subject ?predicate ?object
    } LIMIT 10
    """
    sparql_endpoint = "https://dbpedia.org/sparql"
    mongo_uri = "mongodb://localhost:27017/"
    database_name = "geo_db"
    collection_name = "features"  

    schema_mapping = {
        "subject": "subject",
        "predicate": "predicate",
        "object": "object"
    }

    documents, _ = parse_geosparql_results(sparql_query, sparql_endpoint, schema_mapping)
    print("Parsed Documents: ", documents)  # Έλεγχος των εγγράφων πριν την αποθήκευση
    store_documents_to_mongo(documents, mongo_uri, database_name, collection_name)

# Run the simple test function
test_sparql_query()
