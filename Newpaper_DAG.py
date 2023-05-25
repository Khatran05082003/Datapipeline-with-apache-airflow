from datetime import timedelta
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlalchemy
from datetime import datetime
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def crawl_data():
    url='https://arxiv.org/list/cs.AI/recent'
    html=requests.get(url)
    soup = BeautifulSoup(html.text, "html.parser")
    a=soup.find("dl")
    x='https://arxiv.org'
    links=[]
    for k in a.find_all('dt'):
        links.append(x+k.find_all('a',href=True)[0].get('href'))
    title=[]
    author=[]
    abstract=[]
    subject=[]
    link_download=[]
    for link in links:
        html=requests.get(link)
        soup = BeautifulSoup(html.text, "html.parser")
        title.append(soup.title.text[12:])
        author.append(soup.find('div',class_='authors').find('a').text)
        abstract.append(soup.find('blockquote',class_='abstract mathjax').text.replace('Abstract:','').replace('\n','').strip())
        subject.append(soup.find('span',class_='primary-subject').text)
        link_download.append(x+soup.find('div',class_='full-text').find_all('a',href=True)[0].get('href'))


    data=pd.DataFrame({'Title':title,'Authors':author,'Abstract':abstract,'Subjects':subject,'DownloadUrl':link_download})
    return data



def load_to_mysql(data):
    username='root'
    password='0508'
    database='test'
    host='localhost'
    db=mysql.connector.connect(user=username,password=password,host=host,database=database)
    mycursor=db.cursor()
    sql_query = """
        CREATE TABLE IF NOT EXISTS newspaper(
            Title VARCHAR(200),
            Authors VARCHAR(200),
            Abstract VARCHAR(1000),
            Substracts VARCHAR(200),
            DownloadUrl VARCHAR(200)
        )
        """
    mycursor.execute(sql_query)
    print("Opened database successfully")
    engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}')
    data.to_sql('newspaper', con=engine, if_exists='replace', index=False)

default_arg={
    'owner':'khaTran',
    'retries':5,
    'retry_daily':timedelta(minutes=2)
}


with DAG(
    dag_id='my_second_dag',
    default_args=default_arg,
    description='This is our first dag that we write',
    start_date=datetime(2023,5,25,1),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='crawl_data',
        python_callable=crawl_data
    )
    
    task2 = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_to_mysql
    )

    task1 >> task2

