import requests
from datetime import datetime, timedelta
from copy import copy
import pandas as pd


class PrometheusAPI:
    def __init__(self, url="https://prometheus_host.ru:9090", **kwargs):
        """
        Класс для получения данных с хоста Prometheus

        :param url: Prometheus url
        :param kwargs: параметры запроса
        """
        self.url = url
        self._api_path = self.url + "/api/v1/query_range"
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        query_params = {"step_s": 20, "timedelta_min": 2}
        query_params.update(kwargs)
        self.params = query_params

    def __str__(self):
        return f"Prometheus API: {self.url}"

    def get_cpu_usage(self, host: str, **kwargs) -> pd.DataFrame:
        """
        Метод для получения данных о нагрузке на CPU

        :param host: необходимый хост
        :param kwargs: параметры запроса
        :return: ДатаФрейм с данными
        """
        query = (
            f'100 - avg(irate(node_cpu_seconds_total{{host=~"{host}", mode = "idle"}}[5m])) *100'
        )
        response = self._request(query=query, **kwargs)
        return self.transform_data_to_df(response)

    def get_system_load(self, host: str, minutes: int, **kwargs) -> pd.DataFrame:
        """
        Метод для получения данных о средней нагрузке на систему

        :param host: необходимый хост
        :param minutes: параметр скользящего среднего
        :param kwargs: параметры запроса
        :return: ДатаФрейм с данными
        """
        query = f' node_load{minutes}{{host=~"{host}"}}'
        response = self._request(query=query, **kwargs)
        return self.transform_data_to_df(response)

    def get_memory_usage(self, host: str, **kwargs) -> pd.DataFrame:
        """
        Метод для получения данных о нагрузке на RAM c ClickHouse ноды

        :param host: адрес ch хоста
        :return: ДатаФрейм с данными
        """
        query = f'ClickHouseMetrics_MemoryTracking{{host=~"{host}"}}'
        response = self._request(query=query, **kwargs)
        return self.transform_data_to_df(response)

    def _request(self, query: str, **kwargs) -> dict:
        """
        Метод с HTTP запросом по API

        :param query: запрос query_range
        :param kwargs: параметры запроса
        """
        data = copy(self._headers)
        timedelta_min = kwargs.get("timedelta_min") or self.params["timedelta_min"]
        step = kwargs.get("step_s") or self.params["step_s"]
        start_time = datetime.utcnow() - timedelta(minutes=timedelta_min)
        end_time = datetime.utcnow()
        response = requests.post(
            url=self._api_path,
            data=data,
            params={
                "query": query,
                "start": start_time.isoformat("T") + "Z",
                "end": end_time.isoformat("T") + "Z",
                "step": step,
            },
            timeout=5,
        ).json()
        if not response["data"]["result"]:
            raise ValueError("Empty response, maybe wrong host, or query?")
        return response

    @staticmethod
    def transform_data_to_df(data: dict) -> pd.DataFrame:
        """
        Метод преобразования данных

        :param data: json-словарь с ключами "data" -> "result" -> "values" -> [0,1]
        :return: ДатаФрейм с метрикой и временной меткой
        """
        result_data = pd.json_normalize(data, record_path=["data", "result", "values"])
        result_data[0] = result_data[0].apply(lambda x: pd.Timestamp.fromtimestamp(x))
        result_data[1] = result_data[1].astype("float")
        return result_data.rename(columns={0: "timestamp", 1: "metric"})


def get_least_loaded_node(
    ch_host: str = "ch-{node}-host.ru",
    enabled_nodes: list | tuple = (1, 3),
    default_node: int = 3,
    prometheus: PrometheusAPI = PrometheusAPI(),
) -> int:
    """
    Функция для балансировки нагрузки на ноды ClickHouse

    :param ch_host: строка с Кластером ClickHouse, обязательно наличие в строке {node}, для указания адреса конкретной ноды
    :param enabled_nodes: доступные ноды ClickHouse
    :param default_node: возвращаемое значение ноды по умолчанию, если Prometheus долго не отвечает
    :param prometheus: объект класса PrometheusAPI
    :return: int номер ноды
    """
    system_load = {i: copy([]) for i in enabled_nodes}
    for node in enabled_nodes:
        try:
            memory_load_data = prometheus.get_memory_usage(host=ch_host.format(node=node))
            memory_load_mean = memory_load_data["metric"].mean() / 1024 / 1024
            memory_load_mean = 5 * round(memory_load_mean / 5)
            system_load[node].append(memory_load_mean)
            cpu_load_data = prometheus.get_cpu_usage(host=ch_host.format(node=node))
            cpu_load_mean = cpu_load_data["metric"].mean()
            cpu_load_mean = 5 * round(cpu_load_mean / 5)
            system_load[node].append(cpu_load_mean)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print(f"http request for {prometheus} timed out, node {default_node} was returned")
            return default_node

    load_priority = sorted(system_load, key=system_load.get)
    return load_priority[0]
