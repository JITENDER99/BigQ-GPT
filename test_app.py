import pathlib
import textwrap
import google.generativeai as genai
import json
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_cors import CORS
from IPython.display import display
from IPython.display import Markdown
from urllib.parse import unquote

app = Flask(__name__)
CORS(app)

GOOGLE_API_KEY = 'AIzaSyAmliF5Y6HQhbrbf7_9mb7hrgvZGQuriWg'
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-pro')

# Variables to store user inputs
project = ""
dataset = ""
table = ""

# This function uses default credentials for gcp and gets data in a df from bigquery table
def big_query(project, dataset, table):
    # Importing the library 
    import google.auth, os
    from google.cloud import bigquery
    from google.cloud import storage
    import pandas as pd
    import numpy as np
    import re
    import warnings
    warnings.filterwarnings('ignore')
    import pandas_gbq
    pd.set_option('display.max_rows', None)
    # Setting up credentials
    credentials, _ = google.auth.default()
    credentials = google.auth.credentials.with_scopes_if_required(credentials, bigquery.Client.SCOPE)
    # Initialize Clients
    bigquery_client = bigquery.Client(credentials=credentials)
    storage_client= storage.Client(credentials=credentials)
    project_id= 'eo-dev-data-land-ecom-gl-9574'
        
    query = f"SELECT * from `{project}.{dataset}.{table}` LIMIT 100"
    print(query)
    df = pd.read_gbq(query, dialect="standard", credentials=credentials, project_id=project_id)
    print(df.head()) 
    
    return df   

@app.route('/')
def index():
    return render_template('big_gpt.html')

@app.route('/generate', methods=['POST'])
def generate_content():
    global project, dataset, table

    print("function started")
    if request.method == 'POST':
        project = request.form.get('project', '')
        dataset = request.form.get('dataset', '')
        table = request.form.get('table', '')
        print(project, dataset, table)

        print(f"Received inputs: Project={project}, Dataset={dataset}, Table={table}")
        # Use the variables as needed, for example, pass them to the model
        user_input = f"{project} {dataset} {table}"
        big_data = big_query(project, dataset, table)
        print(big_data)
        # Gen AI call
        response = model.generate_content(f"On the basis of given table {big_data}, kindly provide me the 5 analytical based general questions along with their answers with respect to the mentioned table. Note that  questions should be in a separate list and answers should be in a separate list").text
        #result_text = "\n".join([f"{chunk.text}\n{'_'*80}" for chunk in response])
        print(response)
        result_text=response
        # Redirect to a new route for displaying results
        return redirect(url_for('show_results', result_text=result_text))
        #return jsonify(result_text=result_text)

@app.route('/show_results')
def show_results():
    # Retrieve the result_text from the query parameters
    result_text_encoded = request.args.get('result_text', '')
    result_text = unquote(result_text_encoded)
    result_text=jsonify(result_text)
    #import pdb; pdb.set_trace()
    #result_text=jsonify(result_text)
    #print(result_text.__dict__)
    #print(result_text)
    # Render a template to display the results
    return render_template('result_page.html', result_text=result_text)

if __name__ == '__main__':
    app.run(debug=True)
