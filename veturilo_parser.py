import requests
import dataset
import datetime
import sqlalchemy
import time
from xml.dom import minidom


DB_URL = 'sqlite:///veturilo.db'
BIKE_STOPS_TABLE_NAME = 'bike_stops'
BIKE_IDS_TABLE_NAME = 'bike_ids'
STATION420_ID = 420420


def db_clear():
    with dataset.connect(DB_URL) as tx:
        bike_stops_table = tx[BIKE_STOPS_TABLE_NAME]
        bike_ids_table = tx[BIKE_IDS_TABLE_NAME]

        bike_stops_table.drop()
        bike_ids_table.drop()

        try:
            tx.commit()
        except:
            tx.rollback()


def db_init():
    with dataset.connect(DB_URL) as tx:
        bike_stops_table = tx[BIKE_STOPS_TABLE_NAME]
        bike_ids_table = tx[BIKE_IDS_TABLE_NAME]

        if 'bike_id' not in bike_stops_table.columns:
            bike_stops_table.create_column('bike_id', sqlalchemy.INT)

        if 'bike_id' not in bike_ids_table.columns:
            bike_ids_table.create_column('bike_id', sqlalchemy.INT)

        if 'first_seen' not in bike_stops_table.columns:
            bike_stops_table.create_column('first_seen', sqlalchemy.DATETIME)

        bike_ids_table.create_index(['bike_id'])
        bike_stops_table.create_index(['bike_id', 'station_id', 'first_seen'])

        try:
            tx.commit()
        except:
            tx.rollback()


def download_data():
    r = requests.get('https://nextbike.net/maps/nextbike-official.xml?city=210')
    assert r.status_code == 200
    veturilo_data = open('veturilo.dat', "w")
    veturilo_data.write(r.content)
    veturilo_data.close()


class BikeSeen:
    def __init__(self, bike_id=None, station_id=None):
        self.bike_id = bike_id
        self.station_id = station_id
        self.date = datetime.datetime.now()

    def add_bike_to_data_set(self, transaction):
        station_changes = 0

        bike_stops_table = transaction[BIKE_STOPS_TABLE_NAME]
        bike_ids_table = transaction[BIKE_IDS_TABLE_NAME]

        sql_string = 'SELECT * FROM {} WHERE bike_id={} ORDER BY first_seen DESC LIMIT 1'\
            .format(BIKE_STOPS_TABLE_NAME, self.bike_id)

        results = [x for x in transaction.query(sql_string)]
        result = None
        if len(results) > 0:
            result = results[0]

        data = dict()
        data['bike_id']= self.bike_id
        data['station_id']= self.station_id
        data['last_seen']= self.date
        data['times_seen'] = 1

        if len(results)>0 and result['station_id'] == self.station_id:
            data['first_seen']= result['first_seen']
            data['last_seen']= self.date
            data['times_seen']= result['times_seen'] + 1
            bike_stops_table.update(data, ['bike_id', 'station_id', 'first_seen'])
        else:
            station_changes += 1
            data['first_seen']= self.date
            bike_stops_table.insert(data)

        if bike_ids_table.find_one(bike_id=self.bike_id) is None:
            bike_ids_table.insert({'bike_id': self.bike_id})
            print 'Found a new bike with bike_id = {}'.format(self.bike_id)

        return station_changes


def fetch_data_to_db():
    download_data()
    parsed_xml = minidom.parse('veturilo.dat')
    stations = parsed_xml.getElementsByTagName('place')

    bikes_now = []

    for s in stations:
        if int(s.attributes['bikes'].value) == 0:
            continue

        bike_numbers = s.attributes['bike_numbers'].value.split(',')

        for num in bike_numbers:
            bikes_now.append(BikeSeen(int(num), int(s.attributes['number'].value)))

    with dataset.connect(DB_URL) as tx:
        seen_ids = set([b.bike_id for b in bikes_now])

        all_bike_ids = [x['bike_id'] for x in tx[BIKE_IDS_TABLE_NAME].find()]
        not_seen_bikes_ids = [x for x in all_bike_ids if x not in seen_ids]
        station_changes = 0

        for bike_id in not_seen_bikes_ids:
            bikes_now.append(BikeSeen(bike_id, STATION420_ID))

        for bike_seen in bikes_now:
            station_changes += bike_seen.add_bike_to_data_set(tx)

        try:
            tx.commit()
            print 'Successfully updated {} entries'.format(station_changes)
        except:
            print 'Error Occurred'
            tx.rollback()

print 'Clearing Data from db'
db_clear()

print 'Initializing the db'
db_init()

while True:
    time_start = time.time()

    print 'Fetching data from Veturilo API to db'
    fetch_data_to_db()

    time_finish = time.time()
    time_elapsed = time_finish-time_start

    print 'Done in {} seconds'.format(time_elapsed)

    time_to_wait = max(0.0, 45.0-time_elapsed)

    print 'Time now {}, waiting {} Seconds'.format(datetime.datetime.now().time(), time_to_wait)


    def wait_with_counting(duration):
        while True:
            print '{} seconds left'.format(duration)
            time.sleep(min(10.0, duration))
            duration -= 10.0
            if duration <= 0.0:
                break


    wait_with_counting(time_to_wait)