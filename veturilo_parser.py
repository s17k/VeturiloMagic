import requests
import dataset
from xml.dom import minidom

def download_data():
    r = requests.get('https://nextbike.net/maps/nextbike-official.xml?city=210')
    assert r.status_code == 200
    veturilo_data = open('veturilo.dat', "w")
    veturilo_data.write(r.content)
    veturilo_data.close()


# download_data()

xmldoc = minidom.parse('veturilo.xml')
itemlist = xmldoc.getElementsByTagName('place')

class Bike:
    id=0
    station_id=0
    

bikes = []

for s in itemlist:
    if int(s.attributes['bikes'].value) == 0:
        continue
    print(s.attributes['name'].value, s.attributes['number'].value, s.attributes['bike_numbers'].value)
    bike_numbers = s.attributes['bike_numbers'].value.split(',')
    for num in bike_numbers:
        print('Adding bike bike_id: {} station_id {}'.format(num, s.attributes['number'].value))
        
    

