import cincyquery
from collections import namedtuple
from datetime import datetime
from flask import Flask
from flask_apscheduler import APScheduler
from flask_restful import Resource, Api, reqparse
from globalmaptiles import GlobalMercator
from gtfs_realtime_pb2 import FeedMessage
import json
import math
import png
from redis import Redis
import requests

app = Flask(__name__)
api = Api(app)

class Config(object):
    JOBS = [
        {
            'id': 'fetch_buses',
            'func': 'dangers:fetch_buses',
            'args': (),
            'trigger': 'interval',
            'seconds': 60,
        },
        {
            'id': 'fetch_asteroids',
            'func': 'dangers:fetch_asteroids',
            'args': (),
            'trigger': 'interval',
            'days': 7,
        },
        {
            'id': 'fetch_weather',
            'func': 'dangers:fetch_weather',
            'args': (),
            'trigger': 'interval',
            'minutes': 10,
        },
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

def fetch_asteroids():
    ASTEROIDS_URL = 'https://ssd-api.jpl.nasa.gov/sentry.api?ip-min=1e-3'

    r = requests.get(ASTEROIDS_URL)
    assert r.status_code == 200

    asteroids = r.json()

    redis = open_redis()
    redis.set('asteroids', sum(float(a['ip']) for a in asteroids['data']) + 0.001)

class AsteroidDanger(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('longitude', type=float, location='args')
        parser.add_argument('latitude', type=float, location='args')

        redis = open_redis()
        danger = float(redis.get('asteroids'))

        return {
            "danger": danger,
        }

api.add_resource(AsteroidDanger, '/asteroid')

def fetch_weather():
    WEATHER_URL = 'https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/ridge::CVG-TR0-0/9/135/195.png'

    r = requests.get(WEATHER_URL)
    assert r.status_code == 200

    redis = open_redis()
    redis.set('weather', r.content)

class WeatherDanger(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('longitude', type=float, required=True, location='args')
        parser.add_argument('latitude', type=float, required=True, location='args')

        redis = open_redis()
        p = redis.get('weather')
        if not p:
            return {}
        p = png.Reader(bytes=p)
        width, height, data, attr = p.read()
        data = list(data)

        args = parser.parse_args()
        gm = GlobalMercator()
        minx, miny, maxx, maxy = gm.TileBounds(135, 316, 9)
        mx, my = gm.LatLonToMeters(args.latitude, args.longitude)
        dx = math.floor((mx - minx) / (maxx - minx) * width)
        dy = math.floor((my - miny) / (maxy - miny) * height)
        res = 0.5 * ((maxx - minx) / width + (maxy - miny) / height)
        span = math.ceil(16100 / res) # 10 mile radius

        safety = 1.0
        for y in range(max(0, dy - span), min(height, dy + span)):
            row = data[y]
            if not any(row):
                continue
            for x in range(max(0, dx - span), min(width, dx + span)):
                r, g, b, a = row[4 * x: 4 * x + 4]
                if (a == 0) or (r == g and g == b):
                    continue
                # if r == 108 and g == 125 and b == 170:
                #     intensity = 5.95
                if r >= g and g >= b:
                    intensity = (g - b) / (r - b)
                elif r >= b and b >= g:
                    intensity = (g - b) / (r - g)
                elif g >= r and r >= b:
                    intensity = 2.0 + (b - r) / (g - b)
                elif g >= b and b >= r:
                    intensity = 2.0 + (b - r) / (g - r)
                elif b >= r and r >= g:
                    intensity = 4.0 + (r - g) / (b - g)
                else:
                    intensity = 4.0 + (r - g) / (b - r)

                dist = 1.0 - (abs(x - dx) + abs(y - dy)) / (span + span)
                if intensity < 0.0:
                    safety *= math.erf(4 * (1.0 - dist * (-intensity / 6.0)))
                else:
                    safety *= math.erf(4 * (1.0 - dist * (1.0 - intensity / 6.0)))

        return {
            "danger": 1.0 - safety,
        }

api.add_resource(WeatherDanger, '/weather')

class CincyDanger(Resource):
    def get(self, typ):
        parser = reqparse.RequestParser()
        parser.add_argument('longitude', type=float, required=True, location='args')
        parser.add_argument('latitude', type=float, required=True, location='args')
        args = parser.parse_args()

        redis = open_redis()
        key = '%s-%s-%s' % (typ, str(round(args.latitude, 3)), str(round(args.longitude, 3)))
        cached = redis.get(key)
        if cached:
            return json.loads(cached)

        date = datetime.now()
        date = date.replace(year=date.year - 1)
        result = cincyquery.start(typ, "'%s'" % date.isoformat(), args.latitude, args.longitude)
        if not result:
            cached = redis.get('%s-default' % typ)
            if cached:
                return json.loads(cached)
            return []

        redis.set(key, json.dumps(result), ex=604800)
        redis.set('%s-default' % typ, json.dumps(result), ex=604800)
        return result

api.add_resource(CincyDanger, '/cincy/<string:typ>')

if __name__ == '__main__':
    app.config.from_object(Config())

    scheduler = APScheduler()
    scheduler.api_enabled = True
    scheduler.init_app(app)
    scheduler.start()
    for job in scheduler.get_jobs():
        scheduler.run_job(job.id)

    app.run(debug=False, host='0.0.0.0')
