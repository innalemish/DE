from dotenv import load_dotenv
import os
import shutil
from flask import Flask, request, jsonify
import requests

load_dotenv()

AUTH_TOKEN = os.getenv('AUTH_TOKEN')

app = Flask(__name__)

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

    page = 1
    while True:
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={'date': date, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
        )

        if response.status_code == 200:
            with open(os.path.join(raw_dir, f'sales_{date}_page_{page}.json'), 'w') as file:
                file.write(response.text)
            page += 1
        else:
            break

        return jsonify({"message": "Data saved successfully"}), 201

    
if __name__ == '__main__':
    app.run(port=8081)