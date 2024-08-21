import requests
from pymongo import MongoClient
from datetime import datetime
from shapely.wkt import loads as load_wkt
from shapely.geometry import mapping
import folium

def send_sparql_request(sparql_query, sparql_endpoint):
    params = {
        'query': sparql_query,
        'format': 'application/sparql-results+json'
    }
    response = requests.get(sparql_endpoint, params=params)
    if response.status_code != 200:
        raise Exception(f"SPARQL query failed with status code {response.status_code}")
    return response.json()

# def parse_sparql_results(results, schema_mapping, sparql_query):
#     documents = []
#     for result in results['results']['bindings']:
#         document = {schema_mapping.get(key, key): (value['value'] if 'value' in value else None) for key, value in result.items()}
        
#         if 'geometry' in document:
#             wkt_literal = document['geometry']
#             print(f"Original Geometry WKT: {wkt_literal}")
#             geojson_geometry = wkt_to_geojson(wkt_literal)
#             if geojson_geometry:  # Only set if conversion was successful
#                 document['geometry'] = geojson_geometry
#             else:
#                 print(f"Invalid GeoJSON Geometry: {wkt_literal}")
#         else:
#             document['geometry'] = None  # Explicitly set geometry to None if it doesn't exist
        
#         document['createdDate'] = datetime.utcnow()
#         document['lastUpdated'] = datetime.utcnow()
#         document['retrievalQuery'] = sparql_query
#         documents.append(document)
#     return documents

def parse_sparql_results(results, schema_mapping, sparql_query):
    documents = []
    for result in results['results']['bindings']:
        document = {schema_mapping.get(key, key): (value['value'] if 'value' in value else None) for key, value in result.items()}
        
        if 'geometry' in document and document['geometry']:
            wkt_literal = document['geometry']
            print(f"Original Geometry WKT: {wkt_literal}")
            
            geojson_geometry = wkt_to_geojson(wkt_literal)
            print(f"Converted GeoJSON: {geojson_geometry}")

            if geojson_geometry:
                document['geometry'] = geojson_geometry
            else:
                print(f"Invalid or unsupported Geometry: {wkt_literal}")

        document['createdDate'] = datetime.utcnow()
        document['lastUpdated'] = datetime.utcnow()
        document['retrievalQuery'] = sparql_query
        documents.append(document)
    return documents

def parse_geosparql_results(sparql_query, sparql_endpoint, schema_mapping):
    """Parses GeoSPARQL results and checks for geometry data."""
    sparql_results = send_sparql_request(sparql_query, sparql_endpoint)
    print("SPARQL Results: ", sparql_results)  # Log SPARQL results for debugging
    documents = parse_sparql_results(sparql_results, schema_mapping, sparql_query)
    
    has_geometry = any('geometry' in doc and doc['geometry'] is not None for doc in documents)
    
    return documents, has_geometry


def wkt_to_geojson(wkt_literal):
    try:
        if isinstance(wkt_literal, dict):
            wkt_literal = wkt_literal.get('value', '').strip()
        if not wkt_literal:
            return None
        shape = load_wkt(wkt_literal)
        return mapping(shape)
    except Exception as e:
        print(f"Error converting WKT to GeoJSON: {e}")
        return None
    
def is_valid_geojson(geojson_geometry):
    if not isinstance(geojson_geometry, dict):
        return False
    if 'type' not in geojson_geometry or 'coordinates' not in geojson_geometry:
        return False
    if geojson_geometry['type'] not in ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon']:
        return False
    return True

def store_documents_to_mongo(documents, mongo_uri, database_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    valid_documents = [doc for doc in documents if 'geometry' not in doc or (doc['geometry'] and is_valid_geojson(doc['geometry']))]
    if valid_documents:
        collection.insert_many(valid_documents)
    else:
        print("No valid documents to insert.")

def create_layer(layer_name, features, geometry_type, color='blue'):
    layer = folium.FeatureGroup(name=layer_name)

    for feature in features:
        geometry = feature.get('geometry', {})
        name = feature.get('name', feature.get('subject', 'No Name'))
        
        geo_type = geometry.get('type')
        coords = geometry.get('coordinates', [])

        # For debuggind,to print the geometry type and coordinates
        print(f"Adding {geo_type} with coordinates: {coords}")

        if not coords:
            print(f"Skipping feature with missing coordinates: {feature['_id']}")
            continue

        if geo_type == geometry_type:
            if geometry_type == 'Point' and len(coords) == 2:
                folium.Marker(
                    location=[coords[1], coords[0]],  # Folium uses [lat, long]
                    popup=name,
                    icon=folium.Icon(color=color, icon='info-sign')
                ).add_to(layer)
            elif geometry_type == 'LineString' and coords:
                folium.PolyLine(
                    locations=[[coord[1], coord[0]] for coord in coords],
                    color=color
                ).add_to(layer)
            elif geometry_type in ['Polygon', 'MultiPolygon'] and coords:
                folium.GeoJson(
                    geometry,
                    name="geojson",
                    tooltip=name,
                    style_function=lambda x: {'color': color}
                ).add_to(layer)
    
    return layer

# def run_user_sparql_query(sparql_query, mongo_uri, database_name):
#     sparql_endpoint = "https://dbpedia.org/sparql"
#     schema_mapping = {
#         "subject": "subject",
#         "geometry": "geometry"
#     }
    
#     client = MongoClient(mongo_uri)
#     db = client[database_name]
#     collection = db["features"]
    
#     # Clear the existing collection before running the new query
#     collection.delete_many({})
    
#     documents, has_geometry = parse_geosparql_results(sparql_query, sparql_endpoint, schema_mapping)
#     store_documents_to_mongo(documents, mongo_uri, database_name, "features")

def run_user_sparql_query(sparql_query, mongo_uri, database_name):
    
    sparql_endpoint = "https://dbpedia.org/sparql"
    schema_mapping = {
         "subject": "subject",
         "geometry": "geometry"
    }
    client = MongoClient(mongo_uri)
    db = client[database_name]
    #collection = db["features"]

    # Clear the collection before inserting new data
    #collection.delete_many({})  # This removes all existing documents in the collection

    # Run your SPARQL query, parse the results, and store them
    documents, has_geometry = parse_geosparql_results(sparql_query, sparql_endpoint, schema_mapping)
    if has_geometry:
        store_documents_to_mongo(documents, mongo_uri, database_name, "features")

def fetch_features_from_mongo(mongo_uri, database_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    features = list(collection.find())
    return features

    
def create_layered_map(features):
    """Create a layered map based on features."""
    # Start with a fresh map instance
    map_center = [0, 0]  # Default map center
    m = folium.Map(location=map_center, zoom_start=2)
    
    # Create layers
    parks_layer = folium.FeatureGroup(name='Parks')
    rivers_layer = folium.FeatureGroup(name='Rivers')
    buildings_layer = folium.FeatureGroup(name='Buildings')

    for feature in features:
        geometry = feature.get('geometry', {})
        name = feature.get('name', feature.get('subject', 'No Name'))
        geo_type = geometry.get('type')
        coords = geometry.get('coordinates', [])

        if not coords:
            continue

        if geo_type == 'Point' and len(coords) == 2:
            folium.Marker(
                location=[coords[1], coords[0]],
                popup=name,
                icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(parks_layer)
        elif geo_type == 'LineString' and coords:
            folium.PolyLine(
                locations=[[coord[1], coord[0]] for coord in coords],
                color='blue'
            ).add_to(rivers_layer)
        elif geo_type in ['Polygon', 'MultiPolygon'] and coords:
            folium.GeoJson(
                geometry,
                name="geojson",
                tooltip=name,
                style_function=lambda x: {'color': 'green'}
            ).add_to(buildings_layer)

    # Add layers to the map
    parks_layer.add_to(m)
    rivers_layer.add_to(m)
    buildings_layer.add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    # Return the map object for rendering
    return m



