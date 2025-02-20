import json
from datetime import datetime
from model import TaskModel
from controller import TaskController
import configparser 
import psutil
import os
import re


# Create instances of Model, View, and Controller
model = TaskModel()
controller = TaskController(model)
config = configparser.ConfigParser()
config.read(r'settings/config.txt') 
hq_ip =  config.get('mall_config', 'HQ_IP')
port =  config.get('mall_config', 'Port')
process_to_kill = "create_script.exe"

# get PID of the current process
my_pid = os.getpid()

# iterate through all running processes
for p in psutil.process_iter():
    # if it's process we're looking for...
    if p.name() == process_to_kill:
        # and if the process has a different PID than the current process, kill it
        if not p.pid == my_pid:
            p.terminate()

def get_script():
    compiled = controller.get_data()
    data=""
    if compiled['header_sales']:
        data_delete = compiled['header_sales']['data']['delete']
        data_insert = compiled['header_sales']['data']['insert']
        data +=data_delete
        data +=data_insert
        
    if compiled['hourly_sales']:
        hourly = compiled['hourly_sales']['data']
        for i in hourly:
            data_delete = i['delete']
            data_insert = i['insert']
            data +=data_delete
            data +=data_insert
    if compiled['eod_sales']:
        data_delete = compiled['eod_sales']['data']['delete']
        data_insert = compiled['eod_sales']['data']['insert']
        data +=data_delete
        data +=data_insert
    if compiled['logs']:
        data_delete = compiled['logs']['data']['delete']
        data_insert = compiled['logs']['data']['insert']
        data +=data_delete
        data +=data_insert
    
    text_file = open("Logs/script/script.sql", "w")
    text_file.write(str(data))
    text_file.close()    
    
    return

get_script()
















             
