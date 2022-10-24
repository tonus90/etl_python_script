import psycopg2
import os

# Параметры соединения
conn_string_source= "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
conn_string_target= "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'" 

# Создаем соединение (оно поддерживает контекстный менеджер, рекомендую пользоваться им)
# Создаем курсор - это специальный объект который делает запросы и получает их результаты
with psycopg2.connect(conn_string_source) as conn, conn.cursor() as cursor:
    # query = 'select * from customer limit 1' # запрос к БД
    query2 = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    cursor.execute(query2) # выполнение запроса
    result = cursor.fetchall() # получение результата
    for element, in result:
        q = f"COPY {element} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open(f'/home/user1/ETL/L5/{element}.csv', 'w') as f:
            cursor.copy_expert(q, f)

for element, in result:
    query_ddl = os.popen(f"docker exec -it my_postgres pg_dump -t {element} --schema-only my_database").read()
    rows_ddl = query_ddl.split('\n')
    rows_ddl.remove(rows_ddl[20])
    query_ddl = '\n'.join(rows_ddl)
    with psycopg2.connect(conn_string_target) as conn, conn.cursor() as cursor:
        cursor.execute(f'drop table if exists {element}')
        a = cursor.execute(query_ddl)


with psycopg2.connect(conn_string_target) as conn, conn.cursor() as cursor:
    query2 = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    cursor.execute(query2) # выполнение запроса
    result = cursor.fetchall()
    for element, in result:
        q = f"COPY {element} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'/home/user1/ETL/L5/{element}.csv', 'r') as f:
            cursor.copy_expert(q, f)

