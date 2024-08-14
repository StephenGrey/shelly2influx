#!/usr/bin/env python3
"""install as a service - shelly_influx"""


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


def influx_read(query):
	with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG) as client:
		query_api = client.query_api()
		result = query_api.query(org=org, query=query)
	return result

"""
Iterate through the tables and records in the Flux object.

Use the get_value() method to return values.
Use the get_field() method to return fields.
results = []
for table in result:
  for record in table.records:
    results.append((record.get_field(), record.get_value()))

print(results)
[(temperature, 25.3)]
The Flux object provides the following methods for accessing your data:

get_measurement(): Returns the measurement name of the record.
get_field(): Returns the field name.
get_value(): Returns the actual field value.
values: Returns a map of column values.
values.get("<your tag>"): Returns a value from the record for given column.
get_time(): Returns the time of the record.
get_start(): Returns the inclusive lower time bound of all records in the current table.
get_stop(): Returns the exclusive upper time bound of all records in the current table.
"""


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

def load_config(configFilename):
    global lagSecs, intervalSecs,INFLUX_URL, BUCKET, ORG, INFLUX_TOKEN,SHELLY_URL,SHELLY_TOKEN,devices,config
    config = {}
    with open(configFilename) as configFile:
        config = json.load(configFile)
    
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
	

def get_update():
    startupTime = datetime.utcnow()
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

    				shelly_type=device['type']
    				
#    				print(shelly_type)
    				
    				#print(f"{device['name']}   Online: {online}")
    				res=requests.post(SHELLY_URL+"status",data={'id':device['id'],'auth_key':SHELLY_TOKEN})
    				
#    				print(res.json())
    				
    				if 'data' in res.json():
    					online=res.json()['data'].get('online')
    					data=res.json()['data']['device_status']
    					updated=data['_updated']
    				else:
    					error(f"No data for device {device}")

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
    					
    					influx_write2({"location":device['location'],"device":device['name']+"#1","online":online},{
    						"power":float(power1['power']),
    						"power_total":float(power1['total']),
    						"updated":updated
    						})
    						
    					influx_write2({"location":device['location'],"device":device['name']+"#2","online":online},{"power":float(power2['power']),"power_total":float(power2['total']),"updated":updated})
    				
    				#	print(f"UNIT2: Power:{power2['power']}W {power2['total']/1000:.2f} kWh  Updated:{updated}")
    				elif shelly_type=='Shelly Uni':
    					temp=data.get('ext_temperature')
    					if temp:
#    						print(temp)
    						if len(temp)==2:
    							temp1=temp['0'].get('tC')
    							temp1_ID=temp['0'].get('hwID')
    							temp2=temp['1'].get('tC')
    							temp2_ID=temp['1'].get('hwID')
    							#print(temp1,temp1_ID,temp2,temp2_ID)
    					else:
    						temp1,temp2=0.0,0.0
    					
    					relays=data.get('relays')
    					if relays:
    						out1=relays[0].get('ison')
    						out2=relays[1].get('ison')
    					else:
    						out1,out2=None,None
    					
    					inputs=data.get('inputs')
    					if inputs:
    						input1_event=inputs[0].get("event")
    						input1_cnt=inputs[0].get("event_cnt")
    						input1_input=inputs[0].get("input")
    						input2_event=inputs[1].get("event")
    						input2_cnt=inputs[1].get("event_cnt")
    						input2_input=inputs[1].get("input")
    					
    					
    					else:
    						input1_event,input1_cnt,input1_input,input2_event,inpu2_cnt,input2_input=None,None,None,None,None,None
    					
    					if 'input:2' in data:
    						counts=data["input:2"].get("counts")
    						if counts:
    							total_count=counts['total']
    						else:
    							total_count=None
    					else:
    						total_count=None
    					
    					acds=data.get('adcs')
    					if acds:
    						ac_input=next(iter(acds or []), None)
    						ac_volts=float(ac_input.get('voltage'))
    					else:
    						ac_volts=0.0
    					#print(f"Output from Uni: {ac_volts} volts")
    					#print(data)
    					influx_write2(
    						{
    					"location":device['location'],"device":device['name']+"#1","online":online
    						},
    						{
    						"temperature":float(temp1),
    						"button":input1_input,
    						"input_event": input1_event,
    						"input_count":input1_cnt,
    						"out":out1,
    						"input_volts":ac_volts,
    						"total_count":total_count
    						}
    						)
    					influx_write2(
    						{
    					"location":device['location'],"device":device['name']+"#2","online":online
    						},
   							{
   							"temperature":float(temp2),
    						"button":input2_input,
    						"input_event": input2_event,
    						"input_count":input2_cnt,
    						"out":out2,
    						"input_volts":ac_volts
    						}
    					)
    					
    				if 'switch:0' in data:
    					output="OFF" if not data['switch:0']['output'] else "ON"
    					if 'apower' in data['switch:0']:
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
#except:
#    error('Fatal error: {}'.format(sys.exc_info())) 
#    traceback.print_exc()
#    
    
if __name__ == "__main__":
	if len(sys.argv) != 2:
		print('Usage: python {} <config-file>'.format(sys.argv[0]))
	else:
		configFilename = sys.argv[1]
		load_config(configFilename)
		get_update()
#    detailedStartTime = startupTime
 
 