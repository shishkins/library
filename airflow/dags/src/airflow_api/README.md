# Airflow API 

Класс для упрощения работы с
[AirflowAPI](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

## Create class instance
```
import os

from src.airflow_api import AirflowAPI

api_instance = AirflowAPI(
    url='http://dv-voyager:8180',
    login=os.getenv('airflow_login'),
    password=os.getenv('airflow_password'),
)
```

```commandline
>>> print(api_instance)
Airflow API instance for http://dv-voyager:8180
```

## GET
```commandline
>>> api_instance.get(endpoint='/variables')
{'total_entries': 64, 'variables': [{'description': 'desc', 'key': 'key', 'value': 'value'}, ...
```

## POST
```commandline
>>> new_var = {'key': 'new_var', 'value': '123', 'description': 'new variable'}
>>> api_instance.post(endpoint='/variables', data=new_var).text
{
  "description": "new variable",
  "key": "new_var",
  "value": "123"
}
```
## PATCH
```commandline
>>> new_var = {'key': 'new_var', 'value': '456', 'description': 'description'}
>>> api_instance.patch(endpoint='/variables/new_var', data=new_var).text
{
  "description": "description",
  "key": "new_var",
  "value": "456"
}
```
## DELETE
```commandline
>>> api_instance.delete(endpoint='/variables/new_var')
<Response [204]>
```

## Example using api with limit and offset
```commandline
>>> api_instance.get(endpoint='/variables?limit=1&offset=64')
{'total_entries': 65, 'variables': [{'description': None, 'key': 'new_var', 'value': '123'}]}
```
