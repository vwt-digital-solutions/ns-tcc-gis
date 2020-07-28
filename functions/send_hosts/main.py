import requests

import config


def get_access_token():
    data = {
        "client_id": config.CLIENT_ID,
        "client_secret": config.CLIENT_SECRET,
        "grant_type": "client_credentials"
    }

    response = requests.post(config.OAUTH_URL, data=data).json()

    return response["access_token"]


def add_feature(x, y, name, hostname, host_groups, layer):
    adds = [
        {
            "geometry": {"x": x, "y": y},
            "attributes": {
                "name": name,
                "hostname": hostname,
                "host_groups": host_groups
            }
        }
    ]

    data = {
        "adds": adds,
        "f": "json",
        "token": get_access_token()
    }

    r = requests.post(config.SERVICE_URL + f"/{layer}/addFeatures", data=data).json()

    return r["addResults"]


def main(request):
    pass
