import datetime
import time
from xml.dom import minidom

import dataset
import requests
import sqlalchemy
import math

DB_URL = 'sqlite:///veturilo.db'
BIKE_STOPS_TABLE_NAME = 'bike_stops'
BIKES_TABLE_NAME = 'bikes'
STATION420_ID = 420420
PRIORS = 1.0/99.0

def db_clear():
    with dataset.connect(DB_URL) as tx:
        bike_stops_table = tx[BIKE_STOPS_TABLE_NAME]
        bike_ids_table = tx[BIKES_TABLE_NAME]

        bike_stops_table.drop()
        bike_ids_table.drop()

        try:
            tx.commit()
        except:
            tx.rollback()


def db_init():
    with dataset.connect(DB_URL) as tx:
        bike_stops_table = tx[BIKE_STOPS_TABLE_NAME]
        bike_ids_table = tx[BIKES_TABLE_NAME]

        def create_column(column_name, column_type, table):
            if column_name not in table.columns:
                table.create_column(column_name, column_type)

        create_column('bike_id', sqlalchemy.INT, bike_stops_table)
        create_column('bike_id', sqlalchemy.INT, bike_ids_table)
        create_column('first_seen', sqlalchemy.DATETIME, bike_stops_table)

        bike_ids_table.create_index(['bike_id'])
        bike_stops_table.create_index(['bike_id', 'station_id', 'first_seen'])
        bike_stops_table.create_index(['bike_id'])

        try:
            tx.commit()
        except:
            tx.rollback()


def download_data():
    r = requests.get('https://nextbike.net/maps/nextbike-official.xml?city=210')
    assert r.status_code == 200
    veturilo_data = open('veturilo.dat', "w")
    veturilo_data.write(r.content.decode("utf-8"))
    veturilo_data.close()


class BikeSeen:
    def __init__(self, bike_id=None, station_id=None):
        self.bike_id = bike_id
        self.station_id = station_id
        self.date = datetime.datetime.now()

    def find_last_occurrence_in_db(self, transaction):
        sql_string = 'SELECT * FROM {} WHERE bike_id={}'\
        ' ORDER BY first_seen DESC LIMIT 1' \
            .format(BIKE_STOPS_TABLE_NAME, self.bike_id)

        results = [x for x in transaction.query(sql_string)]

        result = None

        if len(results) > 0:
            result = results[0]

        return result

    def add_bike_to_data_set(self, transaction):

        bike_stops_table = transaction[BIKE_STOPS_TABLE_NAME]
        bike_ids_table = transaction[BIKES_TABLE_NAME]

        last_occurrence = self.find_last_occurrence_in_db(transaction)

        data = dict()
        data['bike_id'] = self.bike_id
        data['station_id'] = self.station_id
        data['last_seen'] = self.date
        data['times_seen'] = 1

        if last_occurrence and last_occurrence['station_id'] == self.station_id:
            data['first_seen'] = last_occurrence['first_seen']
            data['last_seen'] = self.date
            data['times_seen'] = last_occurrence['times_seen'] + 1

            search_columns = ['bike_id', 'station_id', 'first_seen']
            bike_stops_table.update(data, search_columns)
            update_type = 0
        else:
            data['first_seen'] = self.date
            bike_stops_table.insert(data)
            update_type = 1

            if bike_ids_table.find_one(bike_id=self.bike_id) is None:
                insert_data = {'bike_id': self.bike_id, 'broken_pbb': PRIORS}
                bike_ids_table.insert(insert_data)
                # print('Found a new bike with bike_id = {}'.format(self.bike_id))
                update_type = 2

        return update_type


def fetch_data_to_db():
    download_data()
    parsed_xml = minidom.parse('veturilo.dat')
    stations = parsed_xml.getElementsByTagName('place')

    bikes_now = []

    for station in stations:
        if int(station.attributes['bikes'].value) == 0:
            continue

        bike_numbers = station.attributes['bike_numbers'].value.split(',')
        station_number = int(station.attributes['number'].value)

        for bike_number in bike_numbers:
            int_num = int(bike_number)
            bikes_now.append(BikeSeen(int_num, station_number))

    with dataset.connect(DB_URL) as tx:
        seen_ids = set([b.bike_id for b in bikes_now])

        all_bike_ids = [x['bike_id'] for x in tx[BIKES_TABLE_NAME].find()]
        not_seen_bikes_ids = [x for x in all_bike_ids if x not in seen_ids]

        for bike_id in not_seen_bikes_ids:
            bikes_now.append(BikeSeen(bike_id, STATION420_ID))

        update_type_counters = [0 for i in range(3)]

        for bike_seen in bikes_now:
            update_type = bike_seen.add_bike_to_data_set(tx)
            update_type_counters[update_type] += 1
        try:
            tx.commit()
            print('Data-fetch logs:')

            station_updates = update_type_counters[1]
            print('\tBikes that changed station: {}'.format(station_updates))

            new_bikes = update_type_counters[2]
            print('\tBikes seen for the first time: {}'.format(new_bikes))

            same_station = update_type_counters[0]
            print('\tstayed at the same station: {}'.format(same_station))

            rented_bikes = tx.query('SELECT * FROM {} WHERE station_id={}'\
                               .format(BIKE_STOPS_TABLE_NAME, STATION420_ID))

            print('\tknown rented bikes: {}'\
                  .format(len([x for x in rented_bikes])))
        except:
            print('Error Occurred')
            tx.rollback()


print('Clearing Data from db')
db_clear()

print('Initializing the db')
db_init()

while True:
    time_start = time.time()

    print('Fetching data from Veturilo API to db')
    fetch_data_to_db()

    time_finish = time.time()
    time_elapsed = time_finish - time_start

    print('Done in {} seconds'.format(time_elapsed))

    time_to_wait = max(0.0, 60.0 - time_elapsed)

    print('Time now {}, waiting {} Seconds'.format(datetime.datetime.now().time(), time_to_wait))


    def wait_with_counting(duration):
        while True:
            print('{} seconds left'.format(duration))
            time.sleep(min(10.0, duration))
            duration = 60.0 - time.time() + time_start
            if duration <= 0.0:
                break


    wait_with_counting(time_to_wait)
