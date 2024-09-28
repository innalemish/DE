from flask import Flask, request, jsonify
import os
import json
from fastavro import writer, parse_schema

app = Flask(__name__)

@app.route('/', methods=['POST'])
def convert_to_avro():
    data = request.get_json()
    raw_dir = data.get('raw_dir')
    stg_dir = data.get('stg_dir')

    if not raw_dir or not stg_dir:
        return jsonify({"error": "Missing raw_dir or stg_dir"}), 400

    os.makedirs(stg_dir, exist_ok=True)

    schema = {
        "type": "record",
        "name": "Sales",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "item", "type": "string"},
            {"name": "amount", "type": "float"},
            {"name": "date", "type": "string"}
        ]
    }
    parsed_schema = parse_schema(schema)

    for filename in os.listdir(raw_dir):
        if filename.endswith('.json'):
            with open(os.path.join(raw_dir, filename), 'r') as json_file:
                records = json.load(json_file)
                avro_filename = filename.replace('.json', '.avro')
                with open(os.path.join(stg_dir, avro_filename), 'wb') as avro_file:
                    writer(avro_file, parsed_schema, records)

    return jsonify({"message": "Data converted to Avro successfully"}), 201

if __name__ == '__main__':
    app.run(port=8082)
