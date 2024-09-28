from flask import Flask, request, jsonify # type: ignore
import requests # type: ignore
import os
import json
from datetime import datetime
from dotenv import load_dotenv # type: ignore
from fastavro import writer, parse_schema # type: ignore # new route for converting JSON to Avro

load_dotenv()

app = Flask(__name__)

# URL API
api_url = "https://fake-api-vycpfa6oca-uc.a.run.app/"

# sekret token
auth_token = os.getenv('AUTH_TOKEN')

# schema Avro
schema = {
    "type": "record",
    "name": "Sales",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "float"},
        {"name": "date", "type": "string"}
    ]
}
parsed_schema = parse_schema(schema)

# function to get data from API
def fetch_sales_data(api_url):
    headers = {'Authorization': f'Token {auth_token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# function to convert JSON to Avro
def convert_json_to_avro(json_data, avro_file_path):
    with open(avro_file_path, 'wb') as out:
        writer(out, parsed_schema, json_data)

# for receiving POST requests
@app.route('/fetch_sales', methods=['POST'])
def fetch_sales():
    try:
        data = request.get_json()
        raw_dir = data.get('raw_dir')
        
        if not raw_dir:
            return jsonify({"error": "Parameter 'raw_dir' is required"}), 400
        
        # Cleaning directory before writing new files
        if os.path.exists(raw_dir):
            for file in os.listdir(raw_dir):
                file_path = os.path.join(raw_dir, file)
                if os.path.isfile(file_path):
                    os.unlink(file_path)
        else:
            os.makedirs(raw_dir)
        
        sales_data = fetch_sales_data(api_url)
        timestamp = datetime.now().strftime("%Y-%m-%d")
        file_path = os.path.join(raw_dir, f"sales_{timestamp}.json")
        
        with open(file_path, 'w') as file:
            json.dump(sales_data, file, indent=4)
        
        return jsonify({"message": f"Data successfully saved in {file_path}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# to convert JSON to Avro
@app.route('/convert_to_avro', methods=['POST'])
def convert_to_avro():
    try:
        data = request.get_json()
        raw_dir = data.get('raw_dir')
        stg_dir = data.get('stg_dir')
        
        if not raw_dir or not stg_dir:
            return jsonify({"error": "Parameters 'raw_dir' and 'stg_dir' are required"}), 400
        
        if not os.path.exists(stg_dir):
            os.makedirs(stg_dir)
        
        for file_name in os.listdir(raw_dir):
            if file_name.endswith('.json'):
                json_file_path = os.path.join(raw_dir, file_name)
                with open(json_file_path, 'r') as json_file:
                    json_data = json.load(json_file)
                
                avro_file_name = file_name.replace('.json', '.avro')
                avro_file_path = os.path.join(stg_dir, avro_file_name)
                convert_json_to_avro(json_data, avro_file_path)
        
        return jsonify({"message": f"Data successfully saved in {stg_dir}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=8081)

