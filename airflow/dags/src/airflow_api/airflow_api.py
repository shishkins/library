import os

import requests
from airflow.models import Variable

AIRFLOW_URL = "http://localhost:8180"
AIRFLOW_LOGIN_KEY = "AD_LOGIN"
AIRFLOW_PASSWORD_KEY = "AD_PASSWORD"


class AirflowAPI:
    def __init__(
        self,
        url: str = None,
        login: str = None,
        password: str = None,
    ):
        """
        Инициализатор класса AirflowAPI.

        :param url: строка с url-адресом (http/https) Airflow (str). Пример: http://dv-voyager:8180
        :param login: строка с логином Airflow (str).
        :param password: строка с паролем Airflow (str).
        """
        self.url = url or AIRFLOW_URL
        self.login = login or os.getenv(AIRFLOW_LOGIN_KEY) or Variable.get(AIRFLOW_LOGIN_KEY)
        self.password = (
            password or os.getenv(AIRFLOW_PASSWORD_KEY) or Variable.get(AIRFLOW_PASSWORD_KEY)
        )

        self._auth = (self.login, self.password)
        self._api_path = self.url + "/api/v1"
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def __repr__(self):
        return f"Airflow API instance for {self.url}"

    def get(self, endpoint: str, params: dict = None) -> dict:
        response = requests.get(
            url=self._api_path + endpoint,
            auth=self._auth,
            headers=self._headers,
            params=params,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, json: dict = None) -> dict:
        response = requests.post(
            url=self._api_path + endpoint,
            auth=self._auth,
            headers=self._headers,
            json=json,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    def patch(self, endpoint: str, json: dict = None) -> dict:
        response = requests.patch(
            url=self._api_path + endpoint,
            auth=self._auth,
            headers=self._headers,
            json=json,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    def delete(self, endpoint: str) -> requests.Response:
        response = requests.delete(
            url=self._api_path + endpoint,
            auth=self._auth,
            verify=False,
        )
        response.raise_for_status()
        return response
