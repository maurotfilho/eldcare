from flask import Flask, request, g
from flask_restful import Resource, Api, reqparse
from FlaskDBHelper import DBHelper
from consolidator import Consolidator

DATABASE = 'database.db'
app = Flask(__name__)
db = DBHelper(DATABASE, g, True)
api = Api(app)

class GetData(Resource):
    def get(self):
        rows = db.query('consolidated')

        return {'consolidated': rows[-1]}


class InsertPosition1Data(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('timestamp', type=str, help='timestamp information')
        parser.add_argument('value', type=str, help='position information')
        args = parser.parse_args()
        result = db.insert('position1', args.keys(), args.values())
        return {'result': result}, 200

class InsertPosition2Data(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('timestamp', type=str, help='timestamp information')
        parser.add_argument('value', type=str, help='position information')
        args = parser.parse_args()
        result = db.insert('position2', args.keys(), args.values())
        return {'result': result}, 200

class InsertTemperatureData(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('timestamp', type=str, help='timestamp information')
        parser.add_argument('value', type=int, help='position information')
        args = parser.parse_args()
        result = db.insert('temperature', args.keys(), args.values())
        return {'result': result}, 200

class InsertGasData(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('timestamp', type=str, help='timestamp information')
        parser.add_argument('value', type=int, help='position information')
        args = parser.parse_args()
        result = db.insert('gas', args.keys(), args.values())
        return {'result': result}, 200
		
class HelloWorld(Resource):
    def get(self):
        return 'Hello World'

if __name__ == "__main__":

    api.add_resource(HelloWorld, '/')
    api.add_resource(GetData, '/get_alarm')
    api.add_resource(InsertPosition1Data, '/eldcare/api/position1/insert')
    api.add_resource(InsertPosition2Data, '/eldcare/api/position2/insert')
    api.add_resource(InsertTemperatureData, '/eldcare/api/temperature/insert')
    api.add_resource(InsertGasData, '/eldcare/api/gas/insert')
    app.run(debug=True)


