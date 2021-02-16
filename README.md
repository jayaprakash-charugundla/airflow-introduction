# airflow-introduction

#Create new connection in airflow connections

Conn Id=db_sqlite
Conn Type=Sqlite
Description=SQLITE Connection to DB
Host=/home/airflow/airflow/airflow.db

> airflow tasks test user_processing create_table 2021-1-1
> sqlite3 airflow.db
sqlite> .tables
sqlite> select * from users 