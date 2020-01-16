from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import sys
import os
import gzip
import re 
import uuid
from datetime import datetime

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def input_filter(logfile):
    temp_list = []
    for single_log in logfile:
        filtered_log = line_re.match(single_log)
        if filtered_log is not None:
            temp_list.append(filtered_log.group(0))
    return temp_list
def input_preprocess(list_log):
    output_list = []
    for log in list_log:
        temp_list = []        
        new_list = line_re.split(log)
        new_list = list(filter(None,new_list))
        hostname = new_list[0]
        date_time = new_list[1]
        path = new_list[2]
        byte = int(new_list[3])
        temp_list = [hostname,date_time,path,byte]
        output_list.append(temp_list)
    return output_list
def load_data(input_dir):
    logfile_list = []
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt') as logfile:
            for line in logfile:
                logfile_list.append(line)                
    return logfile_list
def insert_batch(session,logfile):
    insert_user = session.prepare("INSERT INTO nasalogs (host,uuid,bytes,datetime,path) VALUES (?, ?,?,?,?)")
    batch = BatchStatement()
    for single_log in logfile:
        host = single_log[0]
        date_time = datetime.strptime(single_log[1],'%d/%b/%Y:%H:%M:%S') 
        path = single_log[2]
        byte = single_log[3]
        uuid_id = uuid.uuid4()       
        batch.add(insert_user, (host,uuid_id,byte,date_time,path ))
    session.execute(batch)

def main(input_dir,keyspace,table_name):
    #print("input files: ", input_dir)
    logfile = load_data(input_dir)
    filtered_logfile = input_filter(logfile)
    clean_logfile = input_preprocess(filtered_logfile)
    #print("clean logfile: ",clean_logfile[:4])
    
    cluster = Cluster(['199.60.17.32', '199.60.17.65'])
    session = cluster.connect(keyspace)
    
    BATCH_VAL = 100
    
    for index in range(0,len(clean_logfile),BATCH_VAL):
        #print(index, " ;;;; ", index+BATCH_VAL)
        insert_batch(session,clean_logfile[index:index+BATCH_VAL])
    #print(len(clean_logfile),'length')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output_keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(inputs,output_keyspace,table_name)










