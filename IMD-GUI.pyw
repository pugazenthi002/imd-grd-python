import PySimpleGUI as sg
import sys
import imdlib as imd
import csv
import pandas as pd
import os, glob,time
import os.path
from os import path
import requests
from bs4 import BeautifulSoup
url = 'https://www.imdpune.gov.in/cmpg/Griddata/Rainfall_25_Bin.html'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

if response.status_code == 200:
    element = soup.find('select', id='rain')
    dropdown=element.find_all('option')[1]
    zz=dropdown['value']
    print(int(zz))
else:
    print('check internet connection')

def progress():

    playout = [[sg.Text('running '+ str(i+1) +' out of '+ str(len(lats)))],
               [sg.ProgressBar(len(lats), orientation='h', size=(50, 20), key='progressbar')],
               [sg.CloseButton("Close")]]

    window1 = sg.Window('Progress').Layout(playout)
    progress_bar = window1.find_element('progressbar')
    event, values = window1.Read(timeout=0)
    if event == sg.WIN_CLOSED or event == 'Close':
        sys.exit()
    progress_bar.UpdateBar(i + 1)
    time.sleep(2)


os.makedirs("temp", exist_ok=True)
if len(sys.argv) == 1:
    sg.ChangeLookAndFeel('GreenTan')
    form = sg.FlexForm('IMD-GRD-EXTRACT', default_element_size=(40, 1))
    choices = list(reversed(range(1901,int(zz)+1)))
    layout = [
        [sg.Text('Downloading IMD gridded Data as CSV Files', size=(40, 1), font=("Helvetica", 14))],
        [sg.Text('choose the time range to download',size=(30,1))],
        [sg.Text('Start year',size=(10,1), justification='center'),sg.Text('End year',size=(10,1),justification='center')],
        [sg.InputCombo((choices),size=(10, 4)),sg.InputCombo((choices),size=(10, 4))],
        [sg.Text('Coordinates input file (csv file with lat as fist coloumn and lon as second coloumn)', size=(40,2))],
        [sg.In(), sg.FileBrowse()],
        [sg.Checkbox('TMAX', default=True, key='tmaxcheck'), sg.Checkbox('TMIN', key='tmincheck'),sg.Checkbox('RAIN', key='raincheck')],
        [sg.Submit(), sg.Cancel()]
        ]
    button, values = form.Layout(layout).Read()
    if button == sg.WIN_CLOSED or button == 'Cancel':
        form.close()
        sys.exit()
    start_yr=values[0]
    end_yr=values[1]
    csv_file = values[2]
    filt=[values['tmaxcheck'],values['tmincheck'],values['raincheck']]
lats=[]
lons=[]
with open(csv_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        lats.append(float(row.get('lat')))
        lons.append(float(row.get('lon')))
params = ["tmax","tmin","rain"]
pars=[i for indx,i in enumerate(params) if filt[indx]]

form.close()

for i in range(len(lats)):
    for variable in pars: 
        os.makedirs(variable, exist_ok=True)
        lat=lats[i]
        lon=lons[i]
        latshortname=str(f'{lats[i]:.2f}')
        lonshortname=str(f'{lons[i]:.2f}')
        latlongname=str(f'{lats[i]:.5f}')
        lonlongname=str(f'{lons[i]:.5f}')
        
        for y in range(start_yr,end_yr+1,1):
            if not path.exists(variable + "\\" + str(y) + ".GRD"):
                #print("file: " + variable + "\\" + str(y) + ".GRD" + " doesn't exist, downloading from web")
                data = imd.get_data(variable, y, y, fn_format='yearwise')
                data = imd.open_data(variable, y, y,'yearwise')
                data.to_csv("temp\\" + variable + '-' + str(y) + '.csv', lat, lon,)
                continue
            #print("imd-get- " + str(y))
            data = imd.open_data(variable, y, y,'yearwise')  #data = imd.open_data(variable, start_yr, end_yr,'yearwise')   #ds = data.get_xarray()
            data.to_csv("temp\\" + variable + '-' + str(y) + '.csv', lat, lon,)
        fl = glob.glob(os.path.join("temp\\" + variable + "-*"+ latshortname + '_' + lonshortname + '.csv'))
        cb_var=pd.concat([pd.read_csv(f) for f in fl])
        cb_var.to_csv("temp\\" + variable + '_' + latshortname + '_' + lonshortname + '.csv', index=False)
        for f in fl:
            os.remove(f)
    tmaxf=True
    tminf=True
    rainf=True
    try:
        f1=pd.read_csv("temp\\" + 'tmax_' + latshortname + '_' + lonshortname + '.csv')
    except FileNotFoundError: 
        #print("tmax not needed")
        tmaxf = False
    try:
        f2=pd.read_csv("temp\\" + 'tmin_' + latshortname + '_' + lonshortname + '.csv')
    except FileNotFoundError:
        #print("tmin not needed")
        tminf = False
    try:
        f3=pd.read_csv("temp\\" + 'rain_' + latshortname + '_' + lonshortname + '.csv')
    except FileNotFoundError:
        #print("tmax not needed")
        rainf = False
    if (tmaxf) and (tminf) and (rainf):    
        mer1=f1.merge(f2,on='DateTime')
        mer2=mer1.merge(f3,on='DateTime')
        mer2.to_csv(latlongname + '_' + lonlongname + '.csv',header=['Date','Tmax','Tmin','Rain'],index=False)
    elif (not tmaxf) and (tminf) and (rainf):
        mer2=f2.merge(f3,on='DateTime')
        mer2.to_csv(latlongname + '_' + lonlongname + '.csv',header=['Date','Tmin','Rain'],index=False)
    elif (tmaxf) and (not tminf) and (rainf):
        mer1=f1.merge(f3,on='DateTime')
        mer1.to_csv(latlongname + '_' + lonlongname + '.csv',header=['Date','Tmax','Rain'],index=False)
    elif (not tmaxf) and (not tminf) and (rainf):
        #print("only-rain")
        f3.to_csv(latlongname + '_' + lonlongname + '.csv',header=['Date','Rain'],index=False)
    elif (tmaxf) and (tminf) and (not rainf):
        mer1=f1.merge(f2,on='DateTime')
        mer1.to_csv(latlongname + '_' + lonlongname + '.csv',header=['Date','Tmax','Tmin'],index=False)
    else:
        sys.exit()
        #print("nothing to do")
    filelist = glob.glob(os.path.join("*_"+ latlongname + '_' + lonlongname + '.csv'))
    for f in filelist:
        os.remove(f)
    #print("created the file: " + latlongname + '_' + lonlongname + '.csv and deleted the temp files') 
    for f in os.listdir("temp"):
        os.remove(os.path.join("temp", f))
    
    progress()
sg.Popup('done downloading')
#sg.close
