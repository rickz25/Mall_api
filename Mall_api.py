from tkinter import *
from PIL import ImageTk, Image
from tkinter.font import Font
import os, socket, schedule, time, threading, configparser, logging, socket, psutil, aiohttp, asyncio, json, dataclasses
from datetime import datetime, date
from model import TaskModel
from controller import TaskController
from aiohttp_retry import RetryClient, ExponentialRetry
from compile import QueryBuilder
from decimal import Decimal


# kill process when double run the program
process_to_kill = "Mall_api.exe"
# get PID of the current process
my_pid = os.getpid()
# iterate through all running processes
for p in psutil.process_iter():
    # if it's process we're looking for...
    if p.name() == process_to_kill:
        # and if the process has a different PID than the current process, kill it
        if not p.pid == my_pid:
            p.terminate()

# Create instances of Model, View, and Controller
model = TaskModel()
compiler = QueryBuilder()
controller = TaskController(model)
datenow = datetime.today().strftime('%Y-%m-%d')


# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# set colours
bg_color = "#f24f00"
fg_color = "#2A1C06"
title_color = "white"
fg_color_result = "#CBCFCB"
fg_color_text = "#686C68"

config = configparser.ConfigParser()
config.read(r'settings/config.txt') 

hq_ip =  config.get('mall_config', 'HQ_IP')
port =  config.get('mall_config', 'Port')
scheduleStart =  config.get('mall_config', 'task_schedule')
mallcode = config.get('mall_config', 'Mall_Code')

maintenance_sync = config.get('mall_config', 'Maintenance_sync')

token = "Bearer eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTY1NDc1NDg1NCwiaWF0IjoxNjU0NzU0ODU0fQ.p6WAfLuC39cMk3XEF4LcU5iZy1rzbL0VTKVpTY7mRGQ"

if scheduleStart == "":
    scheduleStart=5
else:
   scheduleStart = int(scheduleStart)

if maintenance_sync == "":
    maintenance_sync=0
else:
   maintenance_sync = int(maintenance_sync)

def clear_widgets(frame):
    # select all frame widgets and delete them
    for widget in frame.winfo_children():
        widget.destroy()

#Telnet
def telnet2(ip, port):
	s = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
	try:
		s.connect((ip , int(port)))
		s.shutdown(2)
		return True
	except:
		return False 

def load_frame1(param):
    clear_widgets(frame1)
    # stack frame 2 above frame 1
    frame1.tkraise()
    button = Button(frame1, text='Manual Sync',font=buttonFont, width=12, bg="#999999", highlightthickness=0, bd=0, fg="white", cursor="hand2", command=lambda:trigger())
    button.grid(row=5, column=1, pady=1,padx=119,sticky=W)
    label=Label(frame1, text=param,fg=title_color,font=titleFont, bg=bg_color)
    label.place(relx=0.5, rely=0.5, anchor=CENTER)

# # Function sync for sales
# async def post_request():
#     controller.post_request_controller()
            
# # Function sync for maintenance
# async def request_maintenance():
#     controller.request_maintenance_controller()

def default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S") 
    if isinstance(obj, Decimal):
        return str(obj)
    if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
    raise TypeError ("Type %s not serializable" % type(obj))

def parsing_date(text):
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')

async def post_request():

    get_sync_table = model.getSyncTable()
    if get_sync_table:
        for i in get_sync_table:
            d = parsing_date(i['startdate'])
            start_date = d.strftime('%Y-%m-%d')
            d2 = parsing_date(i['enddate'])
            end_date = d2.strftime('%Y-%m-%d')
            tablename = i['table_name']
            summary_table = model.perSummaryTable(tablename, start_date, end_date)
            try:
                url = f"http://{hq_ip}:{port}/api/post-sales-integration"

                data = compiler.build_query(summary_table, tablename)
                datas = data.split(";")  
                model.updateSyncTable(tablename) #update status to 1 when getting data
                json_data = json.dumps(datas, default=default)
                headers = {
                            'Content-Type': 'application/json',
                            'accept': 'application/json',
                            "Authorization": token
                        }
                conn = aiohttp.TCPConnector(limit=0)
                timeout = aiohttp.ClientTimeout(total=600)
                async with aiohttp.ClientSession(trust_env=True, version = aiohttp.http.HttpVersion10, connector=conn, timeout=timeout) as session:
                    retry_client = RetryClient(session, retry_options=ExponentialRetry(attempts=3), raise_for_status=[408, 500, 502, 503])
                    response = await retry_client.post(url, data=json_data, headers=headers)
                    await asyncio.sleep(1)
                    if response.ok:
                        model.deleteSyncTable(tablename) #delete record when response is success
                    else:
                        logger.exception("Exception occurred: %s", str(response.text()))
                    await retry_client.close()
            except Exception as e:
                logger.exception("Exception occurred: %s", str(e))

# Function sync for maintenance
async def request_maintenance():
    try:
        headers = {
                    'Content-Type': 'application/json',
                    'accept': 'application/json',
                    "Authorization": token
                }
        conn = aiohttp.TCPConnector(limit=0)
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(trust_env=True, version = aiohttp.http.HttpVersion10, connector=conn, timeout=timeout) as session:
            if maintenance_sync ==1:
                global mallcode
                mallcode = mallcode.replace(" ", "")
                for code in mallcode.split(","):
                    url = f"http://{hq_ip}:{port}/api/post-maintenance"
                    postData={}
                    postData["mallcode"]=code
                    response2 = await session.post(url, data=json.dumps(postData), headers=headers)
                    responseData = await response2.json()
                    await asyncio.sleep(5)
                    res = json.loads(responseData)
                    if response2.ok:
                        if res['status']==0:
                            result = controller.post_maintenance(res)
                        else:
                            logger.exception("Exception occurred: %s", res['message'])
                    else:
                        logger.exception("Exception occurred: %s", str(result))
    except Exception as e:
        logger.exception("Exception occurred: %s", str(e))

async def main():
    # Gather task to sync and wait the other process
    try:
        await asyncio.gather(post_request(), request_maintenance())
    except Exception as e:
        logger.exception("Exception occurred: %s", str(e))
    finally: 
        await asyncio.gather(post_request())

def trigger():
    result = telnet2(hq_ip, int(port)) 
    if result:
        load_frame1('Syncing...')
        print('sync')
    else:
        load_frame1('Waiting for a connection, Server Started.')
        print('not sync')
        return
    asyncio.run(main())
        
# initiallize app with basic settings
root = Tk()
root.title('')
root.iconbitmap('assets/icon/settings.ico')
root.eval("tk::PlaceWindow . center")
root.configure(background=bg_color)
root.geometry('350x200')

image = Image.open("assets/icon/bottom_logo.png")
 
# Resize the image using resize() method
resize_image = image.resize((90, 25))
 
img = ImageTk.PhotoImage(resize_image)
 
# create label and add resize image
label1 = Label(image=img,bg=bg_color)
label1.image = img
label1.grid(row=1, column=0,pady=1, padx=10, sticky=W)

# load custom fonts
titleFont = Font(
    family="arial",
    size=12,
    weight="bold"
)
buttonFont = Font(
    family="arial",
    size=11,
    
)
labelFont = Font(
    family="arial",
    size=12,
    
)
resultFont = Font(
    family="arial",
    size=12,
    
)
normalFont = Font(
    family="arial",
    size=12,
    
)

# create a frame widgets
frame1 = Frame(root, bg=bg_color)
root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(0, weight=1)

# place frame widgets in window
frame1.grid(row=0, column=0, sticky="nsew")
    
# load the first frame
load_frame1('Not started.')


##Schedule cron job
try:
    schedule.every(scheduleStart).seconds.do(trigger)
    # schedule.every(scheduleStart).minutes.do(schedule_start)

    def check_schedule():
        while True:
            schedule.run_pending()
            time.sleep(1)

    threading.Thread(target=check_schedule, daemon=True).start()
except Exception as e:
   logger.error('Error at %s', 'Scheduler', exc_info=e)

# start_time = time.time()

logging.getLogger('schedule').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('aiohttp_retry').setLevel(logging.WARNING)


# def minimizeWindow():
#     root.withdraw()
#     root.overrideredirect(False)
#     root.iconify()

# def disable_event():
#     pass

# root.resizable(False, False)
# root.protocol("WM_DELETE_WINDOW", minimizeWindow)

# run app
root.mainloop()