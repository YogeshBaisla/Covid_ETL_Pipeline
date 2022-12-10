from jproperties import Properties
import pymysql
import os
import json
import time
import datetime
config = Properties()
start = time.time()
with open('/home/yogesh/airflow/dags/Covid_Project_Code/codeconfig.properties', 'rb') as config_file:
    config.load(config_file)

def sendToRDS():
    path = config.get('DataPath')[0] + '/'
    files = os.listdir(config.get('DataPath')[0])

    mydb = pymysql.connect(host=config.get('host')[0],port=int(config.get('port')[0]),user=config.get('user')[0],passwd=config.get('password')[0])
    cur = mydb.cursor()
    cur.execute('use covid_project_db;')

    create_query = "create table if not exists CovidProject(sno int default null,state_name varchar(100) default null,active int default null, positive int default null,cured int default null, death int default null, new_active int default null, new_positive int default null,new_cured int default null, new_death int default null, death_reconsille int default null, total int default null, state_code int default null, actualdeath24hrs int default null, Repo_Date date default null);"

    cur.execute(create_query)

    mydb.commit()

    lis = []

    path = config.get('DataPath')[0] + '/'
    files = os.listdir(config.get('DataPath')[0])
    for file in files:
        try:
            dates=file[:10]
            try:
                repo_date = datetime.datetime.strptime(dates,"%Y-%m-%d")
            except:
                try:
                    repo_date = datetime.datetime.strptime(dates,"%d-%m-%Y")
                except:
                    repo_date = datetime.datetime.strptime(dates,"%Y-%d-%m")
            json_data = open(path+file).read()
            json_obj = json.loads(json_data)
            for x in json_obj:
                keyyy = {'sno' : '', 'state_name' : '', 'active':'', 'positive':'', 'cured':'', 'death':'', 'new_active':'', 'new_positive':'', 'new_cured':'', 'new_death':'', 'death_reconsille':'', 'total':'', 'state_code':'', 'actualdeath24hrs':''}
                for key in x.keys():
                    keyyy[key] = x[key]
                    keyyy['Repo_Date']=repo_date
                lis.append(tuple(keyyy.values()))
        except:
            pass
    query = "insert into CovidProject values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cur.executemany(query,lis)
    mydb.commit()
