from flask import Flask, make_response, jsonify

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route('/vendas/', methods=['GET'])
def get_vendas():
    return make_response(jsonify(['dados de vendas']))

app.run()