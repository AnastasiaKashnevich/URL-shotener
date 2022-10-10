from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException
from psycopg2 import OperationalError
from pydantic import BaseModel
import pyshorteners
import psycopg2
import uvicorn
import validators

host = "localhost:8080"
s = pyshorteners.Shortener()

#рассмотрим локальный вариант
url_storage_1_local = {}
url_storage_2_local = {}

def get_compressed_url(original_url):
    if (original_url in url_storage_1_local):
        return url_storage_1_local[original_url]
        # print("value from dictionary:", url_storage_1[original_url])
    else:
        compressed_url = urlparse(s.tinyurl.short(original_url))._replace(netloc=host, scheme='http').geturl()
        url_storage_1_local[original_url] = compressed_url
        # print("value added to dictionary:", url_storage_1[original_url])

    return url_storage_1_local[original_url]


def get_original_url(compressed_url):
    for key, value in url_storage_1_local.items():
        if compressed_url == value:
            # print("key from dictionary:", key)
            return key
    if (compressed_url in url_storage_2_local):
        return url_storage_2_local[compressed_url]
        # print("value from dictionary:", url_storage_2[compressed_url])
    else:
        original_url = s.tinyurl.expand(compressed_url)
        url_storage_2_local[compressed_url] = original_url
        # print("value added to dictionary:", url_storage_2[compressed_url])

    return url_storage_2_local[compressed_url]

get_compressed_url('http://www.google.com')
get_original_url('https://clck.ru/323uv8')

#подключаемся к posqresql
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

#connection
#connection = create_connection("postgres", "postgres", "postgres", "localhost", "5432")

#создание базы данных
def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

create_database_query = "CREATE DATABASE sm_app"
#пока закомментируем, чтобы не было ошибки о повторе создания
#create_database(connection, create_database_query)

#connection
connection = create_connection("sm_app", "postgres", "postgres", "localhost", "5432")

#функция для исполнения различных запросов
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

#подготовка для создания первой таблицы
#create_url_storage = """ CREATE TABLE url_storage_1(id serial PRIMARY KEY, key varchar(256) NOT NULL, value varchar(256) NOT NULL)"""
#пока закомментируем, чтобы не было ошибки о повторе создания
#execute_query(connection, create_url_storage)

#проверка записи
#insert_test_value = """
#    INSERT INTO url_storage_1 (key, value) VALUES ('http://www.google.com','http://localhost:8080/8wa5w2o')
# """
#пока закомментируем, чтобы не было ошибки о повторе создания
#connection.autocommit = True
#cursor = connection.cursor()
#cursor.execute(insert_test_value)

#извлечение данных
def execute_read_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")

#connection.autocommit = True
#url_example = 'http://www.google.com'
#select_value = (f"SELECT value FROM url_storage_1 where key = '{url_example}' ")
#values = execute_read_query(connection, select_value)

#for value in values:
#    print(value)

#попробуем заполнить первую таблицу для сокращения
# original = input()
# select_original = (f"SELECT value FROM url_storage_1 where key = '{original}' ")
# values_org = execute_read_query(connection, select_original)

def get_compressed_url_db(original_url):
    select_original = (f"SELECT value FROM url_storage_1 where key = '{original_url}' ")
    values_org = execute_read_query(connection, select_original)
    if (len(values_org) != 0):
        print("get_compressed_url_db: get from db", values_org[0])
        return values_org[0]
    else:
        compressed_url = urlparse(s.tinyurl.short(original_url))._replace(netloc=host, scheme='http').geturl()
        insert_value =( f"INSERT INTO url_storage_1 (key, value) VALUES ('{original_url}','{compressed_url}')")
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_value)
        print("get_compressed_url_db: added to db", compressed_url)
        return compressed_url

#print(get_compressed_url_db('https://stackoverflow.com'))
#print(get_compressed_url_db('https://yandex.ru'))
#print(get_compressed_url_db('https://google.com'))

def get_original_url_db(compressed_url):
    select_compressed = (f"SELECT key FROM url_storage_1 where value = '{compressed_url}' ")
    values_comp = execute_read_query(connection, select_compressed)
    if (len(values_comp) != 0):
        print("get_original_url_db: get from db", {values_comp[0][0] : compressed_url})
        return values_comp[0][0]
    else:
        original_url = s.tinyurl.expand(urlparse(compressed_url)._replace(netloc='tinyurl.com', scheme='https').geturl())
        insert_value = (f"INSERT INTO url_storage_1 (key, value) VALUES ('{original_url}','{compressed_url}')")
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_value)
        print("get_original_url_db: added to db", {original_url : compressed_url})
        return original_url

#print(get_original_url_db("cqxs9v3v"))
#print(get_original_url_db("http://localhost:8080/o5pyxfm"))
#print(get_original_url_db("http://localhost:8080/mbq3m"))


app = FastAPI()

def raise_bad_request(message):
    raise HTTPException(status_code=400, detail=message)

# store = 1 - LOCAL
# store = 0 - DB
store = 0
MSG_STORE_VALUE_ERROR = "incorrect store parameter value"

# Обработка GET реквеста, отправленного на эндпоинт "/" + значение_сжатой_урлы"
# Возвращает оригинальную урлу в боди респонса
@app.get("/{compressed_url_value}")
def get_original(compressed_url_value):
    compressed_url = "http://localhost:8080/" + compressed_url_value

    if store == 0:
        return get_original_url_db(compressed_url)
    elif store == 1:
        return get_original_url(compressed_url)
    else:
        raise_bad_request(message=MSG_STORE_VALUE_ERROR)

class URLBase(BaseModel):
    target_url: str

# Обработка POST реквеста, отправленного на эндпоинт "/"
# В боди реквеста передается оригинальная урла
# В боди респонса возвращается сжатая урла
@app.post("/")
def get_compressed(url: URLBase):
    if not validators.url(url.target_url):
        raise_bad_request(message="Your provided URL is not valid")
    if store == 0:
        return get_compressed_url_db(url.target_url)
    elif store == 1:
        return get_compressed_url(url.target_url)
    else:
        raise_bad_request(message=MSG_STORE_VALUE_ERROR)

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8080)