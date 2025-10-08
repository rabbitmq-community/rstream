import urllib.parse

import requests
from requests.auth import HTTPBasicAuth


def http_api_get_connections(port: int = 15672) -> list:
    request = "http://localhost:{}/api/connections".format(port)
    response = requests.get(request, auth=HTTPBasicAuth("guest", "guest"))
    response.raise_for_status()
    return response.json()


def http_api_get_connection(name: str) -> bool:
    request = "http://guest:guest@localhost:15672/api/connections/" + urllib.parse.quote(name)
    response = requests.get(request, auth=HTTPBasicAuth("guest", "guest"))
    if response.status_code == 404:
        return False
    return True


def http_api_connection_exists(connection_name: str, connections: list) -> bool:
    for connection in connections:
        if connection["client_properties"]["connection_name"] == connection_name:
            return True
    return False


def http_api_count_connections_by_name(connection_name: str, port: int = 15672) -> int:
    count = 0
    connections = http_api_get_connections(port)
    for connection in connections:
        if connection["client_properties"].get("connection_name") == connection_name:
            count += 1
    return count


def http_api_delete_connection(name: str) -> int:
    request = "http://guest:guest@localhost:15672/api/connections/" + urllib.parse.quote(name)
    response = requests.delete(request, auth=HTTPBasicAuth("guest", "guest"))
    return response.status_code
