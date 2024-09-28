from dotenv import load_dotenv
import os
import shutil
from flask import Flask, request, jsonify
import requests

load_dotenv()

AUTH_TOKEN = os.getenv('AUTH_TOKEN')

app = Flask(__name__)

# sekret token
AUTH_TOKEN = os.getenv('AUTH_TOKEN')

# 2

# 3 joba's idempotence
import shutil

# for receiving POST requests
@app.route('/', methods=['POST'])
# function to get data from API
def fetch_sales():
    data = request.get_json()
    date = data.get('date')
    raw_dir = data.get('raw_dir')

    if not date or not raw_dir:
        return jsonify({"error": "Missing date or raw_dir"}), 400

    # cleaning directory before writing new files
    if os.path.exists(raw_dir):
        shutil.rmtree(raw_dir)
    os.makedirs(raw_dir, exist_ok=True)

    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': date, 'page': 1},
        headers={'Authorization': AUTH_TOKEN},
    )

    if response.status_code == 200:
        with open(os.path.join(raw_dir, f'sales_{date}.json'), 'w') as file:
            file.write(response.text)
        return jsonify({"message": "Data saved successfully"}), 201
    else:
        return jsonify({"error": "Failed to fetch data"}), response.status_code
    
if __name__ == '__main__':
    app.run(port=8081)