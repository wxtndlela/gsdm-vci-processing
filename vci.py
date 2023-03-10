import os
import pandas as pd
import json
from http.server import SimpleHTTPRequestHandler
from http.server import HTTPServer
from urllib.parse import urlparse
import os
import openpyxl
import numpy as np
import cgi
from io import BytesIO
import socket
from datetime import timezone, datetime, timedelta 

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import storage
import urllib.request
from datetime import timedelta
from datetime import date
from google.cloud import storage
import requests
import os
import time


#Access Firebase DB
cred = credentials.Certificate("gsdm-smart-dashboard-firebase-adminsdk-3dc2m-da7a791029.json")
firebase_admin.initialize_app(cred,  {'storageBucket': 'gsdm-smart-dashboard.appspot.com/vci'})
db = firestore.client()

print("[INFO] Connected to DB...")

def process_file(url):  

    #Update VCI data
    col_ref = db.collection('vci') 
    results = col_ref.where(u'processed', u'==', False).get() # one way to query
    for item in results:

        doc = col_ref.document(item.id) # doc is DocumentReference
        field_updates = {"state": "Processing..."}
        doc.update(field_updates) 

    response = requests.get(url)
    filename = 'dw_vcifinal.xlsx'

    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f'{filename} downloaded successfully.')
    else:
        print(f'Error {response.status_code} occurred.') 
        return        

    # Read the local Excel file
    vci_data = pd.read_excel(filename)
    vci_data.fillna(0, inplace=True)

    vci_recs = len(vci_data)
    vci_col_recs = len(vci_data.columns.values)
    cols = vci_data.columns.values

    print(f"[INFO]...Records found: {vci_recs}")


    col_names = [   ['TEXTURE','DEG',0.0, 5.0, 5.0],
        ['VOIDS','EXT',0.0, 5.0, 5.0],
        ['SURFACE_FAILURE_DEG','DEG',6.5, 5.0, 5.0],
        ['SURFACE_FAILURE_EXT','EXT',6.5, 5.0, 5.0],
        ['SURFACE_CRACK_DEG','DEG',5.0, 5.0, 5.0],
        ['SURFACE_CRACK_EXT','EXT',5.0, 5.0, 5.0],
        ['AGGR_LOSS_DEG','DEG',2.0, 5.0, 5.0],
        ['AGGR_LOSS_EXT','EXT',2.0, 5.0, 5.0],
        ['BINDER_CONDITION_DEG','DEG',3.0, 5.0, 5.0],
        ['BINDER_CONDITION_EXT','EXT',3.0, 5.0, 5.0],
        ['BLEEDING_DEG','DEG',3.0, 5.0, 5.0],
        ['BLEEDING_EXT','EXT',3.0, 5.0, 5.0],
        ['SURF_DEFORM_DEG','DEG',8.0, 5.0, 5.0],
        ['SURF_DEFORM_EXT','EXT',8.0, 5.0, 5.0], 
        ['BLOCK_CRACK_DEG','DEG',6.0, 5.0, 5.0],
        ['BLOCK_CRACK_EXT','EXT',6.0, 5.0, 5.0], 
        ['LONG_CRACK_DEG','DEG',4.5, 5.0, 5.0],
        ['LONG_CRACK_EXT','EXT',4.5, 5.0, 5.0],
        ['TRANSVERSE_CRACK_DEG','DEG',4.5, 5.0, 5.0],
        ['TRANSVERSE_CRACK_EXT','EXT',4.5, 5.0, 5.0],
        ['CROCODILE_CRACK_DEG','DEG',10.0, 5.0, 5.0],
        ['CROCODILE_CRACK_EXT','EXT',10.0, 5.0, 5.0],
        ['PUMPING_DEG','DEG',10.0, 5.0, 5.0],
        ['PUMPING_EXT','EXT',10.0, 5.0, 5.0],
        ['RUTTING_DEG','DEG',8.0, 5.0, 5.0],
        ['RUTTING_EXT','EXT',8.0, 5.0, 5.0],
        ['UNDULATION_DEG','DEG',4.0, 5.0, 5.0],
        ['UNDULATION_EXT','EXT',4.0, 5.0, 5.0], 
        ['PATCHING_DEG','DEG',8.0, 5.0, 5.0],
        ['PATCHING_EXT','EXT',8.0, 5.0, 5.0],
        ['POTHOLES_DEG','DEG',15.0, 5.0, 5.0],
        ['POTHOLES_EXT','EXT',15.0, 5.0, 5.0],
        ['EDGE_BREAK_DEG','DEG',3.5, 4.0, 3.0],
        ['EDGE_BREAK_EXT','EXT',3.5, 4.0, 3.0],
        ['RIDING_QUAL_DEG','DEG',5.5, 4.0, 3.0],
        ['SKID_RESISTANCE_DEG','DEG',3.0, 4.0, 3.0]]
    
    def get_val(n_index, column_name):
        return vci_data[column_name][n_index]

    def check_weightvalue_if_found(col_name):
        for col_n in col_names:
            if col_n[0] == col_name:
                return col_n[2]
        return 0.0
    
    def check_Dnx_Enx_if_found(col_name):
        for col_n in col_names:
            if col_n[0] == col_name:
                return col_n[3], col_n[4]
        return 0.0,0.0
    
    A = 0.02509
    B = 0.0007568

    def get_Fn(n_index):
        list_of_cols = []
        Fnsum = 0.0
        Fnmaxsum = 0.0
        for column_name in col_names:

            s = str(column_name[0])

            _fn_ext = 0.0
            _fn_deg = 0.0
            _Wn = 0.0
            _Dn = 0.0
            _Dnx = 0.0
            _En = 0.0
            _Enx = 0.0
            process = False

            _tmp_name = s.replace("_EXT","")
            _tmp_name = _tmp_name.replace("_DEG","")

            if _tmp_name not in list_of_cols:
                list_of_cols.append(_tmp_name)
            else:
                _tmp_name = ''
                
            _out = np.array(col_names)

            #Work out EXT first
            _ext = _tmp_name+'_EXT'
            if _ext == column_name:
                process = True

            if _ext in _out:
                #Do Calculations
                _Wn = check_weightvalue_if_found(_ext)

                #Processing for AGGREGADE LOSS
                if _tmp_name == 'AGGR_LOSS':
                    if get_val(n_index, 'AGGR_LOSS_ACT') == 'A':
                        _Wn = 4.0
                    else:
                        _Wn = 2.0


                _En = get_val(n_index, _ext)

                #If its EDGE BREAK, RIDING QUALITY OR SKID RESISTANCE
                if _ext == 'EDGE_BREAK_EXT':
                    _En = 3

                _Dnx, _Enx = check_Dnx_Enx_if_found(_ext)
            else:
                _En = 0.0

            
            #Work out DEG
            _deg = _tmp_name+'_DEG'
            if _deg == column_name:
                process = True

            if _deg in _out:
                #Do Calculations
                _Wn = check_weightvalue_if_found(_deg)

                #Processing for AGGREGADE LOSS
                if _tmp_name == 'AGGR_LOSS':
                    if get_val(n_index, 'AGGR_LOSS_ACT') == 'A':
                        _Wn = 4.0
                    else:
                        _Wn = 2.0
                        
                _Dn = get_val(n_index, _deg)


                #There is no Degree less than 3 for POTHOLES -- Make this adjustment
                if type(_Dn) == int:
                    if _Dn < 3 and _deg == 'POTHOLES_DEG':
                        _Dn = 3
                else:
                    _Dn = 1

                #==========================

                #If its EDGE BREAK, RIDING QUALITY OR SKID RESISTANCE
                if _deg == 'RIDING_QUAL_DEG' or _deg == 'SKID_RESISTANCE_DEG':
                    _En = 3

                #==========================

                _Dnx, _Enx = check_Dnx_Enx_if_found(_deg)
            else:
                _Dn = 0.0

            if _tmp_name != '':
                Fn = _Dn * _En * _Wn
                Fnsum = Fnsum + Fn
                Fnmax = _Dnx * _Enx * _Wn
                Fnmaxsum = Fnmaxsum + Fnmax
                txt = _tmp_name+" Fn: {Fn:.2f}, Fnmax: {Fnmax:.2f}"
                #print(txt.format(Fn=Fn, Fnmax=Fnmax))
        #print("==========================================") 
        out = "∑Fn: "+str(Fnsum)+", ∑Fnmax: "+str(Fnmaxsum)       
        #out = "∑Fn: {Fnsum:.2f}, ∑Fnmax: {Fnmaxsum:.2f}"       
        #print(out.format(Fnsum=Fnsum, Fnmaxsum=Fnmaxsum))   

        return Fnsum, Fnmaxsum, out
    
    n = vci_recs
    #n = 2
    i = 0
    ind = 1

    while i < n:
        Fns, Fnxs, Text = get_Fn(i)
        C = 1/Fnxs
        #print("C:", C)
        VCIp = 100*(1 - (C*Fns))
        #print("VCIp:", VCIp)
        _Vcia = A*VCIp
        #print("_Vcia:", _Vcia)
        _Vcib = B*(pow(VCIp,2)) #pow is SQUARED => pow(base,exp)
        _Vci = _Vcia +_Vcib
        VCI = pow(_Vci,2).round(1)
        #print("VCI:", VCI)

        #print("==========================================")
        # print(get_val(i,'ROAD_ID'))

        out = str(ind)+"/"+str(n)+" ... ∑Fn: {Fnsum:.10f}, ∑Fnmax: {Fnmaxsum:.10f}, C: {C:.10f}, VCIp: {VCIp:.10f}, VCI: {VCI:.10f}"       
        print(out.format(Fnsum=Fns, Fnmaxsum=Fnxs, C=C, VCIp=VCIp, VCI=VCI)) 

        #Update VCI Value on table
        vci_data.loc[i, ['VCI']] = [VCI]
        i+=1
        ind+=1

    file_path = 'updated_vci.xlsx'
    print("[INFO]...updating vci file...")
    print("[INFO]...Done!")
    # Save the updated Excel file
    vci_data.to_excel(file_path, index=False)

    # Upload updated Excel to storage
    print("[INFO]...uploading vci file...")
    #Update VCI data
    col_ref = db.collection('vci') 
    results = col_ref.where(u'processed', u'==', False).get() # one way to query
    for item in results:

        doc = col_ref.document(item.id) # doc is DocumentReference
        field_updates = {"state": "Uploading updated VCI file..."}
        doc.update(field_updates) 

    storage_client = storage.Client.from_service_account_json('gsdm-smart-dashboard-firebase-adminsdk-3dc2m-da7a791029.json', project='gsdm-smart-dashboard')

    bucket = storage_client.bucket('gsdm-smart-dashboard.appspot.com')
    blob = bucket.blob('road-inspection/vci/'+file_path)
    blob.upload_from_filename(file_path)

    # Opt : if you want to make public access from the URL
    blob.make_public()

    now = datetime.now()

    s1 = now.strftime("%Y%m%d%H%M%S")

    print("[INFO]...vci file upload completed")
    print("[INFO]...updating vci records...")

    #Update VCI data
    col_ref = db.collection('vci') 
    results = col_ref.where(u'processed', u'==', False).get() # one way to query
    for item in results:

        doc = col_ref.document(item.id) # doc is DocumentReference
        field_updates = {"state": "Updating vci records..."}
        doc.update(field_updates) 

    print("[INFO]...vci records updated.")

    if os.path.exists(filename):
        os.remove(filename)
        print(f'{filename} deleted successfully.')
    else:
        print(f'{filename} does not exist.')

    if os.path.exists(file_path):
        os.remove(file_path)
        print(f'{file_path} deleted successfully.')
    else:
        print(f'{file_path} does not exist.')

    print("[INFO] VCI Update Completed!")

    #Update VCI data
    col_ref = db.collection('vci') 
    results = col_ref.where(u'processed', u'==', False).get() # one way to query
    for item in results:

        doc = col_ref.document(item.id) # doc is DocumentReference
        field_updates = {"processed": True, "file_url": blob.public_url, "processed_date": s1, "state": ""}
        doc.update(field_updates)

    
    



if __name__ == '__main__':
    # Read url from Firebase
    #GET VCI Excel to process from Firebase
    col_ref = db.collection('vci') # col_ref is CollectionReference

    while True:
    # Your code here
    
        results = col_ref.where(u'processed', u'==', False).get() # one way to query

        for item in results:
            fileURL = item.to_dict()['file_url']
            process_file(fileURL)
        
        print("[INFO] Checking for new file records!")
        
        time.sleep(300)  # Wait for 5 minutes
    
    
