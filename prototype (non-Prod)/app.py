from obspy.clients.fdsn import Client
from obspy import UTCDateTime
import schedule
import time
from datetime import datetime
from obspy.clients.seedlink import Client as SeedlinkClient
from obspy.clients.seedlink.easyseedlink import create_client
import pytz


# client = Client("GEOFON")
# client dan port
client = SeedlinkClient("geofon.gfz-potsdam.de", 18000)

def get_data():
    # Get the current time in UTC
    utc_time = datetime.now(pytz.UTC)
    formatted_time = utc_time.strftime('%Y-%m-%dT%H:%M:%S')
    time = UTCDateTime(formatted_time)

    # mengambil start time 5 menit lebih awal dari waktu yang diambil
    starttime = time - 10*60
    # mengambil end time waktu saat ini
    endtime = time

    # format stasiun sensor seismograph
    net = "GE"
    stasiun = "JAGI"
    loc = "*"
    chanel = "BH*"
    #stasiun jawa tengah
    st = client.get_waveforms(net,stasiun,loc,chanel,starttime,endtime,attach_response = True)
    # for ch in st:
    #     print(ch.stats.channel)
    #     print(ch.data,"\n")
    print(st, "\n")

# callback
def handle_data(trace):
    print('Data Channel:')
    print(trace.stats.channel)
    print(len(trace))

def log_data(data, type="LOG"):
    print(f"[+] <{type}> {data}")

def get_data_seed_link():
    # Get the current time in UTC
    utc_time = datetime.now(pytz.UTC)
    formatted_time = utc_time.strftime('%Y-%m-%dT%H:%M:%S')
    starttime = UTCDateTime(formatted_time)
    endtime = starttime

    st = client.get_waveforms("GE", "JAGI", "*", "BH*", starttime - 60*2, endtime - 60)

    for ch in st:
        # log_data(f"Received {ch.stats.npts} data(s)")
        # log_data(f"from {ch.stats.starttime} to {ch.stats.endtime}")
        # log_data(f"Delta: {ch.stats.delta}")
        log_data(f"Received {ch.data} data(s)")

    # client = create_client('geofon.gfz-potsdam.de',handle_data)  
    # client.select_stream('GE', 'JAGI', 'BH?')  
    # return client.stat
    # client.run() 

    # get_data_seed_link()


# get_data_seed_link()
# schedule.every(2).seconds.do(get_data_seed_link)

while True:
    try:
        get_data_seed_link()
        # schedule.run_pending()
        # time.sleep(1)
    except:
        print("No Trace Data\n")
        time.sleep(1)
