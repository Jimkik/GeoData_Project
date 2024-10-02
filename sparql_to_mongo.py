import requests
from pymongo import MongoClient
from datetime import datetime
from shapely.wkt import loads as load_wkt
from shapely.geometry import mapping
import folium
import csv
from io import StringIO

# Set a large field size limit (for example, 10 MB)
csv.field_size_limit(10 * 1024 * 1024) 

# Function to test MongoDB connection and list databases and collections.
def test_mongo_connection(mongo_uri, database_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    print(f"Databases: {client.list_database_names()}")
    print(f"Collections in {database_name}: {db.list_collection_names()}")
    
# Function to send a SPARQL query to a specified endpoint and return results in appropriate format.
def send_sparql_request(sparql_query, sparql_endpoint):
    params = {
        'query': sparql_query,
        'format': 'text/csv'  # Request CSV format for the given dataset
    }
    response = requests.get(sparql_endpoint, params=params)
    if response.status_code != 200:
        raise Exception(f"SPARQL query failed with status code {response.status_code}")

    # Parse CSV content into a list of dictionaries.
    csv_data = StringIO(response.text)
    reader = csv.DictReader(csv_data)
    results = [row for row in reader]
    return results

# Function to process the SPARQL results, apply schema mappings, and handle geometries.
def parse_sparql_results(results, schema_mapping, sparql_query):
    documents = []
    for result in results:
        document = {schema_mapping.get(key, key): result[key] for key in result.keys()}
        # Map fields according to schema and process geometry if present
        if 'geometry' in document and document['geometry']:
            wkt_literal = document['geometry']
            geojson_geometry = wkt_to_geojson(wkt_literal)
            if geojson_geometry:
                document['geometry'] = geojson_geometry
        # Extract the name from the subject URL and add metadata.
        subject_parts = document['subject'].split('/')
        document['name'] = subject_parts[-3] if len(subject_parts) >= 5 else 'Unknown'
        document['createdDate'] = datetime.utcnow()
        document['lastUpdated'] = datetime.utcnow()
        document['retrievalQuery'] = sparql_query
        documents.append(document)
    
    return documents

# Wrapper function to send and parse a GeoSPARQL query.
def parse_geosparql_results(sparql_query, sparql_endpoint, schema_mapping):
    sparql_results = send_sparql_request(sparql_query, sparql_endpoint)
    documents = parse_sparql_results(sparql_results, schema_mapping, sparql_query)
    has_geometry = any('geometry' in doc and doc['geometry'] is not None for doc in documents)
    return documents, has_geometry

# Function to convert WKT literal to GeoJSON geometry.
def wkt_to_geojson(wkt_literal):
    try:
        if isinstance(wkt_literal, dict):
            wkt_literal = wkt_literal.get('value', '').strip()
        if wkt_literal.startswith("<http://www.opengis.net/def/crs/"):
            wkt_literal = wkt_literal.split('> ')[-1]
        shape = load_wkt(wkt_literal)
        if shape.is_empty:
            return None
        return mapping(shape)
    except Exception:
        return None

# Validate if the GeoJSON object is valid.
def is_valid_geojson(geojson_geometry):
    if not isinstance(geojson_geometry, dict):
        return False
    if 'type' not in geojson_geometry or 'coordinates' not in geojson_geometry:
        return False
    return geojson_geometry['type'] in ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon']

# Function to store documents to a MongoDB collection.
def store_documents_to_mongo(documents, mongo_uri, database_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    valid_documents = [doc for doc in documents if 'geometry' not in doc or (doc['geometry'] and is_valid_geojson(doc['geometry']))]
    if valid_documents:
        try:
            collection.insert_many(valid_documents)
        except Exception as e:
            print(f"Error inserting documents into MongoDB: {e}")
    else:
        print("No valid documents to insert.")

# Function to run a user-defined SPARQL query and store the results in MongoDB.
def run_user_sparql_query(sparql_query, mongo_uri, database_name):
    sparql_endpoint = "http://localhost:7200/repositories/Ptyxiaki"
    schema_mapping = {
         "subject": "subject",
         "wktLiteral": "geometry"
    }
    documents, has_geometry = parse_geosparql_results(sparql_query, sparql_endpoint, schema_mapping)
    if has_geometry:
        store_documents_to_mongo(documents, mongo_uri, database_name, "features")

# Function to fetch features from a MongoDB collection.
def fetch_features_from_mongo(mongo_uri, database_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    return list(collection.find())

# Create a map layer for features with a specific geometry type.
def create_layer(layer_name, features, geometry_type, color='blue'):
    layer = folium.FeatureGroup(name=layer_name)
    for feature in features:
        geometry = feature.get('geometry', {})
        subject = feature.get('resourceURL', 'No Subject')
        description = feature.get('description', 'No Description available')
        data_info = "<br>".join([f"<b>{d['name']}:</b> {d['value']}" for d in feature.get('data', [])])
        geo_type = geometry.get('type')
        coords = geometry.get('coordinates', [])
        if not coords:
            continue
        popup_content = f"""
        <b>Subject:</b> <a href="{subject}" target="_blank">{subject}</a><br>
        <b>Description:</b> {description}<br>
        {data_info}
        """
        popup = folium.Popup(popup_content, max_width=300)
        tooltip = folium.Tooltip(subject)
        if geo_type == 'Point' and len(coords) == 2:
            folium.Marker(
                location=[coords[1], coords[0]],
                popup=popup,
                tooltip=tooltip,
                icon=folium.Icon(color=color, icon='info-sign')
            ).add_to(layer)
        elif geo_type == 'LineString' and coords:
            folium.PolyLine(
                locations=[[coord[1], coord[0]] for coord in coords],
                color=color,
                popup=popup,
                tooltip=tooltip
            ).add_to(layer)
        elif geo_type in ['Polygon', 'MultiPolygon'] and coords:
            folium.GeoJson(
                geometry,
                name="geojson",
                popup=popup,
                tooltip=tooltip,
                style_function=lambda x: {'color': color}
            ).add_to(layer)
    return layer

# Create a map with multiple layers based on different feature types.
def create_layered_map(features):
    m = folium.Map(location=[43.0, -75.0], zoom_start=5)
    rivers = folium.FeatureGroup(name='Rivers')
    parks = folium.FeatureGroup(name='Parks')
    buildings = folium.FeatureGroup(name='Buildings')
    for feature in features:
        name = feature.get('name', '').lower()
        coordinates = feature['geometry']['coordinates']
        geo_type = feature['geometry']['type']
        popup_content = f"""
        <b>Name:</b> {name}<br>
        <b>Description:</b> {feature.get('description', 'No description available')}
        """
        popup = folium.Popup(popup_content, max_width=300)
        # Assign feature to the correct layer based on its type or name.
        if 'river' in name:
            folium.GeoJson(
                feature['geometry'],
                name="Rivers",
                style_function=lambda x: {'color': 'blue'},
                popup=popup
            ).add_to(rivers)
        elif 'park' in name:
            folium.GeoJson(
                feature['geometry'],
                name="Parks",
                style_function=lambda x: {'color': 'green'},
                popup=popup
            ).add_to(parks)
        else:
            folium.GeoJson(
                feature['geometry'],
                name="Buildings",
                style_function=lambda x: {'color': 'red'},
                popup=popup
            ).add_to(buildings)
    rivers.add_to(m)
    parks.add_to(m)
    buildings.add_to(m)
    folium.LayerControl().add_to(m)
    return m
