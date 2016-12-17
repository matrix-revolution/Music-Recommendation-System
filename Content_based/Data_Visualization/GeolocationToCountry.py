from pygeocoder import Geocoder
import pandas as pd
import time

artistCSV = pd.read_csv('subset_artist_location.txt')

artist_location_dic = {}
lat = []
lon = []
country=[]
count=0

file= open('country1.txt', 'a')

count=0

with open('subset_artist_location.txt') as f:
    for line in f:
        count += 1
        if count>0 :
            print (count)
            valTuple = line.split('<SEP>')
            if len(valTuple) > 1:
                if count%3==0:
                    time.sleep(1.5)
                artistID = valTuple[0]
                artist_location_dic[artistID] = valTuple[1], valTuple[2]
                lat.append(float(valTuple[1]))
                lon.append(float(valTuple[2]))
                results = Geocoder.reverse_geocode(float(valTuple[1]), float(valTuple[2]))
                country.append(results.country)
                file.write(artistID+ "', "+ results.country+"\n")


f.close()

#print (country)
print (count)