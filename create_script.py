import json
from datetime import datetime
from model import TaskModel
from controller import TaskController
import configparser 
import psutil
import os, logging

# Create instances of Model, View, and Controller
model = TaskModel()
controller = TaskController(model)
config = configparser.ConfigParser()
config.read(r'settings/config.txt') 
hq_ip =  config.get('mall_config', 'HQ_IP')
port =  config.get('mall_config', 'Port')
process_to_kill = "create_script.exe"

# Create and configure logger
# logging.basicConfig(filename="Logs/script/script.log",
#                     format='%(asctime)s %(message)s',
#                     filemode='w')
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

logging.basicConfig(
    filename="Logs/script/script.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M")

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

    data=[]
    if compiled['header_sales']:
        data.append(compiled['header_sales']['data'])
    if compiled['hourly_sales']:
        data.append(compiled['hourly_sales']['data'])
    if compiled['eod_sales']:
        data.append(compiled['eod_sales']['data'])
    if compiled['logs']:
        data.append(compiled['logs']['data'])
    logging.info(data)
    return

get_script()
















             
