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


def add_feature(x, y, attributes, layer):
    adds = [
        {
            "geometry": {
                "x": float(x),
                "y": float(y),
                "spatalReference": {
                    "wkid": 4326
                }
            },
            "attributes": attributes
        }
    ]

    res = arcgis_feature("adds", adds, layer)

    return res["addResults"][0]["objectId"]


def delete_feature(data, layer):
    res = arcgis_feature("deletes", data, layer)

    return res


def arcgis_feature(function, data, layer):
    data = {
        function: str(data),
        "f": "json",
        "token": get_access_token()
    }

    return requests.post(config.SERVICE_URL + f"/{layer}/applyEdits", data=data).json()


def do_host(data):
    for host in data["ns_tcc_hosts"]:
        # Check if host is already posted on ArcGIS
        ref = db.collection(u'hosts').document(host["id"])
        doc = ref.get()

        if not doc.exists:
            # If host is not posted then make new feature on ArcGIS and save the ObjectID in the firestore
            try:
                attributes = {
                    "name": host["siteName"],
                    "hostname": host["hostName"],
                    "host_groups": host["hostGroups"],
                    "globalcoverage": host["bssGlobalCoverage"],
                    "hwfamily": host["bssHwFamily"],
                    "lifecyclestatus": host["bssLifecycleStatus"]
                }

                object_id = add_feature(
                    host["longitude"],
                    host["latitude"],
                    attributes,
                    config.LAYER["hosts"]
                )
            except (TypeError, ValueError) as e:
                logging.error(f'Error when adding feature: {e}')
                logging.info(f'Message: {host}')
                continue

            logging.info(f'Successfully added {host["id"]} as feature with object_id {object_id}')

            ref.set({
                u'object_id': object_id,
            })
        else:
            logging.info(f'Feature with id {host["id"]} was already added')


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

    if subscription in config.SUBS["host"]:
        do_host(data)
    elif subscription in config.SUBS["host"]:
        pass
    else:
        logging.info(f'Invalid subscription received: {subscription}')

    return 'OK', 204
