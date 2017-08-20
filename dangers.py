from collections import namedtuple
from flask import Flask
from flask_apscheduler import APScheduler
from flask_restful import Resource, Api, reqparse
from gtfs_realtime_pb2 import FeedMessage
import math
from redis import Redis
import requests

app = Flask(__name__)
api = Api(app)

class Config(object):
    JOBS = [
        {
            'id': 'fetch_buses',
            'func': 'bus:fetch_buses',
            'args': (),
            'trigger': 'interval',
            'seconds': 60,
        }
    ]

def open_redis():
    return Redis()

def fetch_buses():
    POSITIONS_URL = 'http://developer.go-metro.com/TMGTFSRealTimeWebService/vehicle/VehiclePositions.pb'
    UPDATES_URL = 'http://developer.go-metro.com/TMGTFSRealTimeWebService/TripUpdate/TripUpdates.pb'

    r = requests.get(POSITIONS_URL)
    assert r.status_code == 200

    feed = FeedMessage()
    feed.ParseFromString(r.content)

    redis = open_redis()
    redis.geoadd("buses", *(item for entity in feed.entity for item in (
        entity.vehicle.position.longitude,
        entity.vehicle.position.latitude,
        entity.vehicle.vehicle.label)))

class BusDanger(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('longitude', type=float, required=True, location='args')
        parser.add_argument('latitude', type=float, required=True, location='args')

        MAX_DISTANCE = 1.0 # mile
        DISTANCE_UNIT = 'mi'

        args = parser.parse_args()
        redis = open_redis()
        buses = redis.georadius("buses", args.longitude, args.latitude, MAX_DISTANCE,
                                unit=DISTANCE_UNIT, withdist=True, withcoord=True)

        Bus = namedtuple('Bus', ['id', 'distance', 'longitude', 'latitude'])
        def get_buses():
            for bus in buses:
                id, dist, coord = bus
                yield Bus(id=id, distance=dist, longitude=coord[0], latitude=coord[1])

        def get_danger():
            safety = 1.0
            for bus in get_buses():
                safety *= math.erf(bus.distance / MAX_DISTANCE * 8)
            return 1 - safety

        return {
            "buses": [{"id": bus.id,
                       "longitude": bus.longitude,
                       "latitude": bus.latitude} for bus in get_buses()],
            "danger": get_danger(),
        }

api.add_resource(BusDanger, '/bus')

if __name__ == '__main__':
    app.config.from_object(Config())

    scheduler = APScheduler()
    scheduler.api_enabled = True
    scheduler.init_app(app)
    scheduler.start()

    app.run(debug=False)
