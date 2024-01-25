from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    project_name = request.form['project']
    dataset_name = request.form['dataset']
    table_name = request.form['table']
    json_file = request.files['jsonFile']

    # Process the form data and file as needed

    return f"Project: {project_name}, Dataset: {dataset_name}, Table: {table_name}, File: {json_file.filename} uploaded."

if __name__ == '__main__':
    app.run(debug=True)
