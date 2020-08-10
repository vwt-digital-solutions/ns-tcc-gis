import requests

import config


def get_access_token():
    data = {
        "f": "json",
        "username": config.CLIENT_USERNAME,
        "password": config.CLIENT_PASSWORD,
        "request": "gettoken",
        "referer": config.CLIENT_REFERER
    }

    response = requests.post(config.OAUTH_URL, data=data).json()

    return response["token"]


def add_feature(x, y, name, hostname, host_groups, layer):
    adds = [
        {
            "geometry": {
                "x": x,
                "y": y,
                "spatalReference": {
                    "wkid": 4326
                }
            },
            "attributes": {
                "name": name,
                "hostname": hostname,
                "host_groups": host_groups
            }
        }
    ]

    data = {
        "adds": str(adds),
        "f": "json",
        "token": get_access_token()
    }

    r = requests.post(config.SERVICE_URL + f"/{layer}/applyEdits", data=data).json()

    return r["addResults"]


def main(request):
    pass
