from flask import Flask, render_template
import pyodbc
from flask import jsonify
import json
from flask import Response
import datetime

app = Flask(__name__)

conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=ngfwcq3f3\mssqlserver01; DATABASE=NameTestDB;Trusted_Connection=yes")
cursor = conn.cursor()

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()
@app.route('/firstlevel', methods=['GET']) 
def getfirstlevel():     
    cursor = conn.cursor()    
    cursor.execute('SELECT * FROM firstlevelmatch')    
    row_headers=[x[0] for x in cursor.description] #this will extract row headers    
    rv = cursor.fetchall()    
    # conn.close()     
    json_data=[]     
    for result in rv:         
      json_data.append(dict(zip(row_headers,result)))     
    
    return json.dumps(json_data,default=myconverter)
    
@app.route('/secondlevel', methods=['GET']) 
def getsecondlevel():     
    cursor = conn.cursor()    
    cursor.execute('SELECT * FROM secondlevelmatch')    
    row_headers=[x[0] for x in cursor.description] #this will extract row headers    
    rv = cursor.fetchall()    
    # conn.close()     
    json_data=[]     
    for result in rv:         
      json_data.append(dict(zip(row_headers,result)))     
    
    return json.dumps(json_data,default=myconverter)

@app.route('/thirdlevel', methods=['GET']) 
def gethirdlevel():     
    cursor = conn.cursor()    
    cursor.execute('SELECT * FROM thirdlevelmatch')    
    row_headers=[x[0] for x in cursor.description] #this will extract row headers    
    rv = cursor.fetchall()         
    json_data=[]     
    for result in rv:         
      json_data.append(dict(zip(row_headers,result)))     
    
    return json.dumps(json_data,default=myconverter)

@app.route('/manual', methods=['GET']) 
def getmanual():     
    cursor = conn.cursor()    
    cursor.execute('SELECT * FROM manualmatch')    
    row_headers=[x[0] for x in cursor.description] #this will extract row headers    
    rv = cursor.fetchall()        
    json_data=[]     
    for result in rv:         
      json_data.append(dict(zip(row_headers,result)))     
    
    return json.dumps(json_data,default=myconverter)

if __name__ =='__main__':
    app.run(debug=True)
