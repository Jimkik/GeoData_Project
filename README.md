
# GeoData_Project

## Overview
GeoData_Project is an application designed to manage, store, and visualize geospatial data using SPARQL queries, MongoDB, and a web-based interface built with Flask and Folium.

## Prerequisites
- Python 3.8+
- MongoDB
- Git

## Installation Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com/Jimkik/GeoData_Project.git
   cd GeoData_Project
2. **Set Up a Virtual Environment (optional)**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
3. **Install Required Packages**
   ```bash
   pip install flask pymongo requests rdflib folium gunicorn
4. **Configure MongoDB**

-	 Ensure MongoDB is running locally.
-	Update MongoDB connection details in the application’s configuration file if needed.
5. **Run the application**
   ```bash
   python app.py
6. **Access the Application**

	Open a web browser and navigate to `http://localhost:5000`.

## Usage

-   Submit SPARQL queries using the web interface.
-   Visualize geospatial data on interactive maps.
-   Manage users and data through the user interface.

## Contributing

Contributions are welcome! Please fork the repository and create a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License.
