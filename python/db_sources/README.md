# db_sources

pip пакет **db_sources** предназначен для работы с различными источниками данных компании DNS через Python.



## 4. Примеры использования

Из пакета импортируем необходимые классы:

```python
from db_sources import MSSQL, PostgreSQL, ClickHouse
```

Инициализируем классы:

```python
dns_dwh = MSSQL(host='host_name')
fcs = PostgreSQL(host='host_name')
ch = ClickHouse(host='host_name')
```

Параметр **host** является обязательным, подключений по умолчанию не предусмотрено. 

Пример создания классов с остальными параметрами:

```python
dns_dwh = MSSQL(
    host='adm-sql-dwh.partner.ru',
    port=1433,
    database='dns_dwh',
    user='partner.ru\login',
    password='password',
    provide_query=True,
    provide_time=True,
)
fcs = PostgreSQL(
    host='adm-fcs-pg.partner.ru',
    port=5432,
    database='fcs',
    user='login',
    password='password',
    provide_query=True,
    provide_time=True,
)
ch = ClickHouse(
    host='adm-dwh-ch1.dns-shop.ru',
    port=9000,
    database='default',
    user='login',
    password='password',
    provide_query=True,
    provide_time=True,
)
```

* Параметры **provide_query** и **provide_time** отвечают за логирования исходного запроса и затраченного на его 
выполнение времени соответственно (по умолчанию False).

* Для класса MSSQL доступна возможность выбора одного из драйверов: ["pymssql", "pytds", "pytds+ntlm"] 
(**driver=**"pymssql" по умолчанию).

* Для класса MSSQL есть возможность указать параметр **nolock**, который отключит блокировку таблиц при SELECT запросах.

* Класс PostgreSQL поддерживает фабрику строк (параметр **row_factory**) и фабрику курсоров 
(параметр **cursor_factory**) (по умолчанию None).

Извлечём результат запроса в pandas.DataFrame, используя метод **_execute_to_df_**:

```python
dns_dwh.execute_to_df('''select * from access.product_DIM_Product''')
fcs.execute_to_df('''select * from dim.dim_product''')
ch.execute_to_df('''select * from mart_comm.dict_city''')
```

Вставим pandas.DataFrame в таблицу (на примере PostgreSQL):

```python
test_dict = {
    '_dateload': ['2021-01-01 00:00:00'],
    'date': ['2021-01-01'],
    'count': [1]
}
df = pd.DataFrame.from_dict(test_dict)
fcs.insert_df(df=df, schema='public', table='test')
```

Пример использования параметра **external_tables** в методе execute_to_df класса ClickHouse:

```python
external_table = {
    '_dateload': ['2021-01-01 00:00:00'],
    'date': ['2021-01-01'],
    'count': [1]
}
df = pd.DataFrame.from_dict(external_table)
ch.execute_to_df(query='''select * from external_table''', external_tables=[(df, 'external_table')])
```

Параметры **provide_query** и **provide_time** позволяют логировать исходный запрос и время, затраченное на его 
выполнение (на примере MSSQL):

```python
dns_dwh.execute_to_list(query="""select * from test""",
                        provide_query=True,
                        provide_time=True,
                        )
```

Результат выполнения функции:

```commandline
----
|    raw_query : select * from test
| elapsed_time : 0:00:00.016527
```
Стандартные параметры объектов классов всегда можно посмотреть, используя функцию help(db_sources).

## 5. Работа с хранилищем S3

Инициализация класса S3 происходит следующим образом:

```python
from db_sources import S3

s3 = S3(
  endpoint_url='url',
  access_key='key_id',
  secret_key='access_key',
  bucket='bucket_name'
)
```

Дополнительно может быть указан параметр **config** (тип: Config, по умолчанию None).

Класс имеет ряд функций:

```python
# Получение объекта соединения с S3
s3.get_connection()

# Получение кортежа бакетов
s3.get_buckets()

# Получение кортежа файлов в бакете
s3.get_objects()

# Скачивание файла в определенную папку
s3.download(object_name='file_name.xlsx',path='path/to/')

# Получение файла из S3 в виде DataFrame
s3.download_df(object_name='path/to/file')

# Сохранение файла в S3
s3.upload(file='file_name.xlsx', object_name='path/to/file')

# Сохранение файла в S3 в виде .csv файла 
s3.upload_df(df=pd.DataFrame, object_name='path/to/file')

# Удаление файла из бакета
s3.delete(object_name='path/to/file')
```
Все методы кроме s3.get_connection() и s3.get_buckets() принимают дополнительный параметр **bucket** (по умолчанию None),
указывающий в каком из бакетов будет выполняться функция (при отсутствии значения параметра используется бакет, 
указанный при инициализации класса).

Класс имеет возможность обратиться напрямую к объекту S3 (Пакет [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html))

```python
# Создание объекта для бакета
bucket = s3.s3.Bucket('bucket_name')

# Создание объекта для взаимодействия с файлом в S3
file = s3.s3.Object('bucket_name', 'file_name')

# Сохранения файла в бинарном виде в S3
s3.s3.Object('bucket_name', 'file_name').put(Body=file)
```