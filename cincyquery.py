import requests
# import pandas as pd
import sys
import datetime

def location(LA,LO):

    LA = float(LA)
    LO = float(LO)

    LA_start = LA - .007
    LA_end = LA + .007
    if LO < 0:
        LO_Start = LO - .007
        LO_end = LO + .007
    else:
        LO_Start = LO + .007
        LO_end = LO - .007

    return LA_start, LA_end, LO_Start, LO_end

def QueryFood(Date, LA_start, LA_end,LO_Start, LO_end):
    LA_start=str(LA_start)
    LA_end=str(LA_end)
    LO_Start=str(LO_Start)
    LO_end = str(LO_end)


    Query = "$query=SELECT Latitude, Longitude, business_name, action_date, violation_comments WHERE Latitude >= "\
                +LA_start+" and Latitude <= "+LA_end+" and Longitude >= "+LO_Start+" and Longitude <= "+LO_end+\
                " and action_status = 'Not Abated'"+ " and action_date > "+Date+" limit 30"
    Query = Query.replace(" ","%20")
    return Query

def QueryFire(Date, LA_start, LA_end,LO_Start, LO_end):
    LA_start=str(LA_start)
    LA_end=str(LA_end)
    LO_Start=str(LO_Start)
    LO_end = str(LO_end)

    Query = "$query=SELECT latitude_x, longitude_x, neighborhood, dispatch_time_primary_unit,"\
            +" arrival_time_primary_unit, incident_type_desc WHERE LATITUDE_X >= "\
            +LA_start+" and LATITUDE_X <= "+LA_end+" and LONGITUDE_X >= "+LO_Start+" and LONGITUDE_X <= "+LO_end+ \
            " and arrival_time_primary_unit > " + Date+" limit 30"
    Query = Query.replace(" ","%20")
    return Query

def QueryBus(LA, LO):
    LA = str(LA)
    LO = str(LO)
    Query = "longitude="+LO+"&latitude="+LA
    return Query

def start(var,Date, LA,LO):

    LA_start, LA_end, LO_Start, LO_end = location(LA, LO)

    if var == "Food":
        URL = ("https://data.cincinnati-oh.gov/resource/2c8u-zmu9.json?")
        QueryFood2 = QueryFood(Date, LA_start, LA_end, LO_Start, LO_end)
        link = URL + QueryFood2

    elif var == "Fire":
        URL = ("https://data.cincinnati-oh.gov/resource/7zr2-gi5i.json?")
        QueryFire2 = QueryFire(Date, LA_start, LA_end, LO_Start, LO_end)
        link = URL + QueryFire2

    # elif var == "Bus":
    #     URL = ("http://vpn.jmchn.net:5080/bus?")
    #     QueryBus2 = QueryBus(LA, LO)
    #     link = URL + QueryBus2

    response = requests.get(link)
    if response.status_code != 200:
        return None

    data = response.json()
    #print(data)

    #data = pd.DataFrame(data)
    #data.to_csv('out.csv')

    return data

#start("Fire", "'2016-07-25T22:23:55.000'",39.110671,-84.515348)
#start(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])


"""
location(39.110671,-84.515348)
Food = ("https://data.cincinnati-oh.gov/resource/2c8u-zmu9.json?")
Fire = ("https://data.cincinnati-oh.gov/resource/7zr2-gi5i.json?")
"""



