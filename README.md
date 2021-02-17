# airflow-introduction

>pip install 'apache-airflow-providers-sqlite'
#Create new connection: db_sqlite
Conn Id=db_sqlite
Conn Type=Sqlite
Description=SQLITE Connection to DB
Host=/home/airflow/airflow/airflow.db

> airflow tasks test user_processing create_table 2021-1-1
> sqlite3 airflow.db
sqlite> .tables
sqlite> select * from users

>pip install 'apache-airflow-providers-http'
#Create new connection: user-api
Conn Id=user_api
Conn Type=HTTP
Host=https://randomuser.me/

> airflow tasks test user_processing is_api_available 2021-1-1
> airflow tasks test user_processing extracting_user 2021-1-1
> airflow tasks test user_processing processing_user 2021-1-1
> airflow tasks test user_processing storing_user 2021-1-1
> sudo apt update
> sudo apt install postgresql
> sudo -u postgres psql
> ALTER USER postgres PASSWORD postgres

> pip install 'apache-airflow[postgres]'
> airflow db test
> airflow db init
> airflow webserver
> airflow scheduler