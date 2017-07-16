import requests
import dataset
import datetime
import sqlalchemy
import time
from xml.dom import minidom


DB_URL = 'sqlite:///veturilo.db'
TABLE_NAME = 'bike_stops'


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

    def add_bike_to_data_set(self, the_db, the_table):
        sql_string = 'SELECT * FROM {} WHERE bike_id={} ORDER BY first_seen DESC LIMIT 1'\
            .format(TABLE_NAME, self.bike_id)

        results = [x for x in the_db.query(sql_string)]
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
            the_table.update(data, ['bike_id', 'station_id', 'first_seen'])
        else:
            data['first_seen']= self.date
            the_table.insert(data)


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
        table = tx[TABLE_NAME]

        if 'bike_id' not in table.columns:
            table.create_column('bike_id', sqlalchemy.INT)

        if 'first_seen' not in table.columns:
            table.create_column('first_seen', sqlalchemy.DATETIME)

        for bike_seen in bikes_now:
            bike_seen.add_bike_to_data_set(tx, table)
        try:
            tx.commit()
            print 'Successfully added/updated {} entries'.format(len(bikes_now))
        except:
            print 'Error Occurred'
            tx.rollback()

while True:
    print('Fetching data from Veturilo API to db')
    fetch_data_to_db()
    print('Waiting 5 Seconds')
    time.sleep(5)






    

