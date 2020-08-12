import base64
import json
import logging
import requests
import config

from google.cloud import firestore_v1

db = firestore_v1.Client()


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

    return r["addResults"][0]["objectId"]


def new_host(data):

    if isinstance(data, list):
        data = data
    else:
        data = [data]

    for host in data:
        # Check if host is already posted on ArcGIS
        unique_id = host["siteName"] + "_" + host["hostName"]

        ref = db.collection(u'hosts').document(unique_id)
        doc = ref.get()

        if not doc.exists:
            # If host is not posted then make new feature on ArcGIS and save the ObjectID in the firestore
            object_id = add_feature(
                float(host["longitude"]),
                float(host["latitude"]),
                host["siteName"],
                host["hostName"],
                [],
                3
            )

            logging.info(f'Successfully added {unique_id} as feature with object_id {object_id}')

            ref.set({
                u'object_id': object_id,
            })


def main(request):
    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        data = json.loads(bytes)
        subscription = envelope['subscription'].split('/')[-1]

        logging.info(f'Read message from subscription {subscription}')
    except Exception as e:
        logging.error(f'Extracting of data failed: {e}')
        return 'Error', 500

    if data:
        new_host(data)

    return 'OK', 204
