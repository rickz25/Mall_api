from tkinter import *
from PIL import ImageTk
from tkinter import messagebox
from tkinter.font import Font
import subprocess, platform
import socket
import schedule
import time
import threading
import configparser 
import logging
from PIL import Image

from datetime import datetime
from model import TaskModel
from controller import TaskController
import json
import aiohttp
import asyncio

# Create instances of Model, View, and Controller
model = TaskModel()
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


def load_frame1(param):
    clear_widgets(frame1)
    # stack frame 2 above frame 1
    frame1.tkraise()
    button = Button(frame1, text='Manual Sync',font=buttonFont, width=12, bg="#999999", highlightthickness=0, bd=0, fg="white", cursor="hand2", command=lambda:trigger())
    button.grid(row=5, column=1, pady=1,padx=119,sticky=W)
    label=Label(frame1, text=param,fg=title_color,font=titleFont, bg=bg_color)
    label.place(relx=0.5, rely=0.5, anchor=CENTER)

async def post_request():
    try:
        url = f"http://{hq_ip}:{port}/api/post-sales-integration"
        
        json_data = controller.get_data()
       
        headers = {
                    "Content-Type": "application/json",
                    "Authorization": token
                }
        async with aiohttp.ClientSession(trust_env=True, version = aiohttp.http.HttpVersion10) as session:
        
            response = await session.post(url, data=json_data, headers=headers)
            res = await response.json()
            await asyncio.sleep(0)
            if response.ok:
                result = controller.post_data(res)
                if result==None:
                    load_frame1('Syncing...')
                else:
                    load_frame1(result)
            else:
                logger.exception("Exception occurred: %s", str(result))
                load_frame1('Server Error.')

            if maintenance_sync ==1:
                global mallcode
                mallcode = mallcode.replace(" ", "")
                for code in mallcode.split(","):
                    url2 = f"http://{hq_ip}:{port}/api/post-maintenance"
                    postData={}
                    postData["mallcode"]=code
                    response2 = await session.post(url2, data=json.dumps(postData), headers=headers)
                    responseData = await response2.json()
                    await asyncio.sleep(0)
                    res = json.loads(responseData)
                    if response2.ok:
                        if res['status']==0:
                            # print(controller.post_maintenance(res))
                            result = controller.post_maintenance(res)
                            if result=="":
                                load_frame1('Syncing...')
                            else:
                                load_frame1(result)
                        else:
                            logger.exception("Exception occurred: %s", res['message'])
                    else:
                        logger.exception("Exception occurred: %s", str(result))
                        load_frame1('Server Error.')
    except Exception as e:
        logger.exception("Exception occurred: %s", str(e))
        load_frame1('Cannot connect to the server.')


def trigger():
     asyncio.run(post_request())
        
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
# schedule.every(2).seconds.do(load_frame1)
try:
    # schedule.every(scheduleStart).seconds.do(trigger)
    schedule.every(scheduleStart).seconds.do(trigger)
    # schedule.every(scheduleStart).minutes.do(schedule_start)

    def check_schedule():
        while True:
            schedule.run_pending()
            time.sleep(1)

    threading.Thread(target=check_schedule, daemon=True).start()
except Exception as e:
   logger.error('Error at %s', 'Scheduler', exc_info=e)

start_time = time.time()


def minimizeWindow():
    root.withdraw()
    root.overrideredirect(False)
    root.iconify()

def disable_event():
    pass

root.resizable(False, False)
root.protocol("WM_DELETE_WINDOW", minimizeWindow)

# run app
root.mainloop()