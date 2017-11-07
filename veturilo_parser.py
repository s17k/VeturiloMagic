import datetime
import time
from xml.dom import minidom

import dataset
import requests
import sqlalchemy
import math
import os.path


def odds_to_bits(odds):
    return math.log2(odds)


def bits_to_probability(bits):
    odds = math.pow(2, bits)
    probability = odds/(1+odds)
    return probability

USE_TEST_DATA = True
CLEAR_BIKES_TABLE_ON_START = True
DB_URL = 'sqlite:///veturilo.db'
BIKES_TABLE_NAME = 'bikes'
NULL_STATION_ID = 1000000000

HYPOTHESES_LABELS = \
    ['WORKING', 'ANNOYING_TO_USE','VISIBLY_BROKEN', 'INVISIBLY_BROKEN']
PRIORS = [0.8, 0.1, 0.05, 0.05]


def get_bikes_table():
    return dataset.connect(DB_URL)[BIKES_TABLE_NAME]


def db_clear():
    table = get_bikes_table()
    if CLEAR_BIKES_TABLE_ON_START:
        table.drop()


def db_init():
    table = get_bikes_table()

    def create_column_if_not_in_yet(column_name, column_type, a_table):
        if column_name not in table.columns:
            a_table.create_column(column_name, column_type)

    create_column_if_not_in_yet('bike_id', sqlalchemy.INT, table)
    create_column_if_not_in_yet('last_station_id', sqlalchemy.String, table)

    table.create_index(['bike_id'])


def download_data(datafile_index):
    # Currently set to Warsaw's id
    r = requests.get('https://nextbike.net/maps/nextbike-official.xml?city=210')
    assert r.status_code == 200

    veturilo_data = open('veturilo.xml', "w")
    veturilo_data.write(r.content.decode("utf-8"))
    veturilo_data.close()

    test_data = open('test_data/{}.xml'.format(datafile_index), "w")
    test_data.write(r.content.decode("utf-8"))
    test_data.close()


def find_bike_in_db(bike_id):
    return get_bikes_table().find_one(bike_id=bike_id)


class BikeSeen:
    """
    The purpose of BikeSeen is to update the database with newly encountered
    evidence for one bike
    """
    def __init__(self, bike_id=None, station_id=None, date=None):
        self.bike_id = bike_id
        self.station_id = station_id
        self.date = date
        self.bike_in_db = None

    def add_same_station_bike_to_dataset(self, table):
        new_times_seen = self.bike_in_db['times_seen'] + 1

        new_amount_of_evidence = self.bike_in_db['evidence_bits']
        if self.station_id == NULL_STATION_ID:
            if new_times_seen < 4:
                new_amount_of_evidence += odds_to_bits(1/(10*new_times_seen))
            elif new_times_seen < 10:
                new_amount_of_evidence += odds_to_bits(1/(2*(10-new_times_seen)))
            elif new_times_seen < 30:
                new_amount_of_evidence += odds_to_bits(1/1.1)

        table.update(dict(
            bike_id=self.bike_id,
            times_seen=new_times_seen,
            evidence_bits=new_amount_of_evidence,
            last_station_id=self.station_id
        ),['bike_id'])

    def add_diff_station_bike_to_dataset(self, table):
        new_evidence = 0

        # todo add shifting the estimated probability

        table.update(dict(
            bike_id=self.bike_id,
            times_seen=1,
            last_station_id=self.station_id,
            evidence_bits=self.bike_in_db['evidence_bits'] + new_evidence
        ), ['bike_id'])

    def add_new_bike_to_dataset(self, table):
        table.insert(dict(
            bike_id=self.bike_id,
            last_station_id=self.station_id,
            times_seen=1,
            evidence_bits=FAULTY_PRIORS
        ))

    def add_bike_seen_to_data_set(self, transaction):
        trans_bikes_table = transaction[BIKES_TABLE_NAME]
        self.bike_in_db = find_bike_in_db(self.bike_id)

        if self.bike_in_db and \
                        self.bike_in_db['last_station_id'] == self.station_id:
            self.add_same_station_bike_to_dataset(trans_bikes_table)
            return 0
        elif self.bike_in_db:
            self.add_diff_station_bike_to_dataset(trans_bikes_table)
            return 1
        else:
            self.add_new_bike_to_dataset(trans_bikes_table)
            return 2


def get_data(num):
    if USE_TEST_DATA and os.path.isfile('./test_data/{}.xml'.format(num)):
        return minidom.parse('./test_data/{}.xml'.format(num)), True
    else:
        download_data(num)
        return minidom.parse('veturilo.xml'), False


def fetch_data_to_db(num):
    parsed_xml, using_old = get_data(num)

    stations = parsed_xml.getElementsByTagName('place')
    insert_date = datetime.datetime.now()

    bikes_seen = []

    for station in stations:
        if int(station.attributes['bikes'].value) == 0:
            continue

        bike_numbers = station.attributes['bike_numbers'].value.split(',')
        station_number = station.attributes['name'].value

        for bike_number in bike_numbers:
            int_num = int(bike_number)
            bikes_seen.append(BikeSeen(int_num, station_number, insert_date))

    with dataset.connect(DB_URL) as tx:
        seen_ids = set([b.bike_id for b in bikes_seen])
        all_bike_ids = [x['bike_id'] for x in tx[BIKES_TABLE_NAME].find()]
        not_seen_bikes_ids = [x for x in all_bike_ids if x not in seen_ids]

        for bike_id in not_seen_bikes_ids:
            bikes_seen.append(BikeSeen(bike_id, NULL_STATION_ID, insert_date))

        update_type_counters = [0 for _ in range(3)]

        for bike_seen in bikes_seen:
            update_type = bike_seen.add_bike_seen_to_data_set(tx)
            update_type_counters[update_type] += 1

        try:
            tx.commit()
            print_after_logs(update_type_counters)
        except:
            print('Error Occurred')
            tx.rollback()

    return using_old


def get_rented_bikes():
    return bikes_table.find(last_station_id=NULL_STATION_ID)


def print_after_logs(update_type_counters):
    print('Data-fetch logs:')

    station_updates = update_type_counters[1]
    print('\tBikes that changed station: {}'.format(station_updates))

    new_bikes = update_type_counters[2]
    print('\tBikes seen for the first time: {}'.format(new_bikes))

    same_station = update_type_counters[0]
    print('\tstayed at the same station: {}'.format(same_station))

    rented_bikes = get_rented_bikes()

    print('\tknown rented bikes: {}'
          .format(len([x for x in rented_bikes])))


print('Clearing Data from db')
db_clear()

print('Initializing the db')
db_init()

bikes_table = get_bikes_table()

ctr = 0

while True:
    time_start = time.time()

    print('Fetching data from Veturilo API to db')

    using_old_data = fetch_data_to_db(ctr)
    ctr += 1

    time_finish = time.time()
    time_elapsed = time_finish - time_start

    print('Done in {} seconds'.format(time_elapsed))

    time_to_wait = max(0.0, 60.0 - time_elapsed)

    print('Time now {}, waiting {} Seconds'
          .format(datetime.datetime.now().time(), time_to_wait))


    def wait_with_counting(duration):
        while True:

            print('{} seconds left'.format(duration))

            time.sleep(min(10.0, duration))
            duration = 60.0 - time.time() + time_start
            if duration <= 0.0:
                break


    if using_old_data:
        print('Skipping')
    else:
        wait_with_counting(time_to_wait)
