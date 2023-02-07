#!/usr/bin/env python3

from datetime import datetime,timedelta
import json
import signal
import sys
import time
import traceback
from threading import Event
# InfluxDB v1
import requests
from influxdb_client import InfluxDBClient,Point,WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS,ASYNCHRONOUS


def influx_write2(tags,fields):
	with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG) as client:
	    write_api = client.write_api(write_options=SYNCHRONOUS)
	    dictionary = {
	    "measurement": "shelly_devices",
	    "tags": tags,
	    "fields":fields,
	    }
	    write_api.write(BUCKET, ORG, dictionary)


# flush=True helps when running in a container without a tty attached
# (alternatively, "python -u" or PYTHONUNBUFFERED will help here)
def log(level, msg):
    now = datetime.utcnow()
    print('{} | {} | {}'.format(now, level.ljust(5), msg), flush=True)

def info(msg):
    log("INFO", msg)

def error(msg):
    log("ERROR", msg)

def handleExit(signum, frame):
    global running
    error('Caught exit signal')
    running = False
    pauseEvent.set()

def getConfigValue(key, defaultValue):
    if key in config:
        return config[key]
    return defaultValue



try:
    if len(sys.argv) != 2:
        print('Usage: python {} <config-file>'.format(sys.argv[0]))
        sys.exit(1)

    configFilename = sys.argv[1]
    config = {}
    with open(configFilename) as configFile:
        config = json.load(configFile)
        
    startupTime = datetime.utcnow()

    intervalSecs=getConfigValue("updateIntervalSecs", 60)
#    detailedIntervalSecs=getConfigValue("detailedIntervalSecs", 3600)
#    detailedDataEnabled=getConfigValue("detailedDataEnabled", False);
#    info('Settings -> updateIntervalSecs: {}, detailedEnabled: {}, detailedIntervalSecs: {}'.format(intervalSecs, detailedDataEnabled, detailedIntervalSecs))
    lagSecs=getConfigValue("lagSecs", 5)
    INFLUX_URL=getConfigValue("INFLUX_URL",None)
    BUCKET=getConfigValue("BUCKET",None)
    ORG=getConfigValue("ORG",None)
    INFLUX_TOKEN=getConfigValue("INFLUX_TOKEN",None)
    SHELLY_URL=getConfigValue("SHELLY_URL",None)
    SHELLY_TOKEN=getConfigValue("SHELLY_TOKEN",None)
    devices=getConfigValue("devices",[])[0]

#    detailedStartTime = startupTime


 
    running = True

    signal.signal(signal.SIGINT, handleExit)
    signal.signal(signal.SIGHUP, handleExit)

    pauseEvent = Event()

#    intervalSecs=getConfigValue("updateIntervalSecs", 60)
#    detailedIntervalSecs=getConfigValue("detailedIntervalSecs", 3600)
#    detailedDataEnabled=getConfigValue("detailedDataEnabled", False);
#    info('Settings -> updateIntervalSecs: {}, detailedEnabled: {}, detailedIntervalSecs: {}'.format(intervalSecs, detailedDataEnabled, detailedIntervalSecs))
#    lagSecs=getConfigValue("lagSecs", 5)
#    detailedStartTime = startupTime

    while running:
    	now = datetime.utcnow()
    	stopTime = now - timedelta(seconds=lagSecs)

    	try:
    		for device_key in devices:
    			device=devices[device_key]	
    			
    			try:
    				res=requests.post(SHELLY_URL+"status",data={'id':device['id'],'auth_key':SHELLY_TOKEN})
    				online=res.json()['data']['online']
    				shelly_type=device['type']
    				#print(shelly_type)
    				#print(f"{device['name']}   Online: {online}")
    				data=res.json()['data']['device_status']
    				updated=data['_updated']
    				
    				if shelly_type=='T&H':
    					temp=data['tmp']
    				#	print(f"Temp: {temp['value']} {temp['units']}   Last updated:{updated}")
    					t1=float(temp['value'])
    					humidity=float(data['hum']['value'])
    				#	print(f"Humidity {humidity}%")
    					influx_write2({"location":device['location'],"device":device['name'],"online":online},{"temperature":t1,"updated":updated,"humidity":humidity})
    							
    				elif shelly_type=='Shelly Em':#'relays' in data:
    					power1=data['emeters'][0]
    					power2=data['emeters'][1]
    				#	print(f"UNIT1: Power:{power1['power']}W {power1['total']/1000:.2f} kWh  Updated:{updated}")
    					
    					influx_write2({"location":device['location'],"device":device['name']+"#1","online":online},{"power":float(power1['power']),"power_total":float(power1['total']),"updated":updated})
    						
    					influx_write2({"location":device['location'],"device":device['name']+"#2","online":online},{"power":float(power2['power']),"power_total":float(power2['total']),"updated":updated})
    			
    				#	print(f"UNIT2: Power:{power2['power']}W {power2['total']/1000:.2f} kWh  Updated:{updated}")
    			
    				if 'switch:0' in data:
    					output="OFF" if not data['switch:0']['output'] else "ON"
    					power=float(data['switch:0']['apower'])
    					energy=float(data['switch:0']['aenergy']['total'])
    					by_min0=float(data['switch:0']['aenergy']['by_minute'][0])
    					by_min1=float(data['switch:0']['aenergy']['by_minute'][1])
    					by_min2=float(data['switch:0']['aenergy']['by_minute'][2])
    					#Energy consumption by minute (in Milliwatt-hours) for the last three minutes (the lower the index of the element in the array, the closer to the current moment the minute)
    					last_min=data['switch:0']['aenergy']['minute_ts']
    					#Unix timestamp of the first second of the last minute (in UTC)
    				#	print(f"RELAY {output} Power:{power}W  Consumed: {energy:.2f} kWh")
    					influx_write2({"location":device['location'],"device":device['name'],"online":online},{"power":power,"consumed_energy":energy,"updated":updated,"last_min":last_min,"by_min0":by_min0,"by_min1":by_min1,"by_min2":by_min2})
    			except Exception as e:
    				error(f'Exception {e}')

	
    	except:
    		error('Failed to record new usage data: {}'.format(sys.exc_info())) 
    		traceback.print_exc()
    	info('Updated influx from Shelly devices')
    	pauseEvent.wait(intervalSecs)

    info('Finished')
except:
    error('Fatal error: {}'.format(sys.exc_info())) 
    traceback.print_exc()
    
    
