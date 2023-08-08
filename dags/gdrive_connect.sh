#!/bin/bash
fileid="1uNWpKM6seqOgrnuPkMZsrgqDFcvKycLvko8-rqhO"
filename="application_record.csv"
html=`curl -c ./cookie -s -L "https://drive.google.com/uc?export=download&id=${fileid}"`
curl -Lb ./cookie "https://drive.google.com/uc?export=download&`echo ${html}|grep -Po '(confirm=[a-zA-Z0-9\-_]+)'`&id=${fileid}" -o "/opt/airflow/data_raw/${filename}"
