from flask import Flask
import os
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename
import json

from project_4_v7 import p4_process_json

UPLOAD_FOLDER = './uploads'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

def allowed_file(filename, extensions):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions

@app.route('/')
def hello():
    return 'Hello World Project 4'


@app.route('/project4', methods=['POST'])
def project4():
    if 'file' not in request.files:
        return jsonify({ 'error': 'No file provided' }), 400

    file = request.files['file']

    url_api = request.form.get('URL_API', 'http://3.84.244.86:8080/enhancer/chain/PurchaseOrder')
    x_thresh = int(request.form.get('X_Threshold', 0))
    y_thresh = int(request.form.get('Y_Threshold', 10))
    word_special_chars = list(request.form.get('Word_Special_Character', '#*/!'))
    number_special_chars = list(request.form.get('Number_Special_Character', ',.'))
    required_urls = json.loads(request.form.get('Required_Urls', '[]'))
    regex_line_step = int(request.form.get('Regex_Line_Step', 2))

    if file and allowed_file(file.filename, ['json']):
        filename = secure_filename(file.filename)
        path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(path)

        r = p4_process_json(path,
            url_api=url_api,
            x_thresh=x_thresh,
            y_thresh=y_thresh,
            word_special_chars=word_special_chars,
            number_special_chars=number_special_chars,
            required_urls=required_urls,
            regex_line_step=regex_line_step,
            verbose=False,
        )

        return jsonify(r)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, threaded=True, port=5011)
