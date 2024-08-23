from flask import Flask, render_template, request, redirect, url_for, flash, session
from sparql_to_mongo import run_user_sparql_query, fetch_features_from_mongo, create_layered_map
import os
from pymongo import MongoClient
import folium

app = Flask(__name__)
app.secret_key = os.urandom(24)  #to display messages

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "geo_db"
COLLECTION_NAME = "features"
USER_COLLECTION_NAME = "users"  

# Setting up MongoDB collections
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
users_collection = db[USER_COLLECTION_NAME]

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        sparql_query = request.form['query']
        if sparql_query.strip():
            run_user_sparql_query(sparql_query, MONGO_URI, DATABASE_NAME)
            return redirect(url_for('index'))  # Redirect to homepage to refresh the map
        else:
            flash("Please enter a valid SPARQL query.")
            return redirect(url_for('index'))

    features = fetch_features_from_mongo(MONGO_URI, DATABASE_NAME, COLLECTION_NAME) if 'userID' in session else []
    map_html = create_layered_map(features) if features else create_layered_map([])  # Create an empty map if no features
    return render_template('index.html', folium_map=map_html._repr_html_())




@app.route('/login', methods=['POST'])
def login():
    if request.method == 'POST':
        user_identifier = request.form['userID'] or request.form['email']
        
        # Find the user by either userID or email
        user = users_collection.find_one({"$or": [{"userID": user_identifier}, {"email": user_identifier}]})

        if user:
            session['userID'] = user['userID']
            session['userName'] = user['userName']
            session['role'] = user['role']
            flash('Login successful!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Login failed. User not found. Please register if you are a new user.', 'danger')
    
    return redirect(url_for('index'))




@app.route('/map_results')
def map_results():
    features = fetch_features_from_mongo(MONGO_URI, DATABASE_NAME, COLLECTION_NAME)
    
    if features:
        map_html = create_layered_map(features)
        return render_template('map_results.html', folium_map=map_html._repr_html_())
    else:
        # Render an empty map
        empty_map = folium.Map(location=[0, 0], zoom_start=2)  # This will create an empty map
        return render_template('map_results.html', folium_map=empty_map._repr_html_())


@app.route('/manage-layers')
def manage_layers():
    if 'userID' not in session:
        flash("Please log in to manage layers.")
        return redirect(url_for('index'))
    pass

@app.route('/add-feature', methods=['GET', 'POST'])
def add_feature():
    if 'userID' not in session:
        flash("Please log in to add features.")
        return redirect(url_for('index'))
    pass


@app.route('/register', methods=['POST'])
def register():
    if request.method == 'POST':
        userID = request.form['userID']
        userName = request.form['userName']
        email = request.form['email']
        role = 'user' 

        if users_collection.find_one({"$or": [{"userID": userID}, {"email": email}]}):
            flash('User ID or Email already exists. Please log in or choose a different User ID/Email.')
            return redirect(url_for('index'))

        user_data = {
            "userID": userID,
            "userName": userName,
            "email": email,
            "role": role,
            "resourceURL": ""
        }

        try:
            users_collection.insert_one(user_data)
            flash('Registration successful! Please log in.')
            return redirect(url_for('index'))
        except Exception as e:
            flash(f'Error: {str(e)}')
            return redirect(url_for('index'))
    
    return redirect(url_for('index'))

@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    flash('You have been logged out.')
    return redirect(url_for('index'))


@app.route('/users')
def list_users():
    users = list(users_collection.find())
    return render_template('users.html', users=users)

@app.route('/user/add', methods=['GET', 'POST'])
def add_user():
    if request.method == 'POST':
        user_data = {
            "userID": request.form['userID'],
            "userName": request.form['userName'],
            "email": request.form['email'],
            "role": request.form['role'],
            "resourceURL": request.form['resourceURL']
        }

        try:
            users_collection.insert_one(user_data)
            flash('User added successfully!')
        except Exception as e:
            flash(f'Error adding user: {str(e)}')

        return redirect(url_for('list_users'))
    
    return render_template('add_user.html')

@app.route('/user/edit/<userID>', methods=['GET', 'POST'])
def edit_user(userID):
    user = users_collection.find_one({"userID": userID})
    
    if request.method == 'POST':
        updated_data = {
            "userName": request.form['userName'],
            "email": request.form['email'],
            "role": request.form['role'],
            "resourceURL": request.form['resourceURL']
        }
        
        users_collection.update_one({"userID": userID}, {"$set": updated_data})
        flash('User updated successfully!')
        return redirect(url_for('list_users'))
    
    return render_template('edit_user.html', user=user)

@app.route('/user/delete/<userID>', methods=['POST'])
def delete_user(userID):
    users_collection.delete_one({"userID": userID})
    flash('User deleted successfully!')
    return redirect(url_for('list_users'))


if __name__ == "__main__":
    app.run(debug=True)
