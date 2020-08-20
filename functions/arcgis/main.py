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


def update_feature(x, y, attributes, layer):
    updates = [
        {
            "geometry": {
                "x": float(x),
                "y": float(y)
            },
            "attributes": attributes
        }
    ]

    res = arcgis_feature("updates", updates, layer)

    return res["updateResults"]


def delete_feature(object_id, layer):
    data = [
        object_id
    ]

    res = arcgis_feature("deletes", data, layer)

    return res["deleteResults"]


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
        ref = db.collection("hosts").document(host["id"])
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
                logging.error(f"Error when adding feature: {e}")
                logging.info(f"Message: {host}")
                continue

            logging.info(f"Successfully added '{host['id']}' as feature with object_id: {object_id}")

            ref.set({
                "object_id": object_id,
                "siteName": host["siteName"],
                "hostGroups": host["hostGroups"],
                "bssGlobalCoverage": host["bssGlobalCoverage"],
                "bssHwFamily": host["bssHwFamily"],
                "bssLifecycleStatus": host["bssLifecycleStatus"],
                "longitude": host["longitude"],
                "latitude": host["latitude"]
            })
        else:
            # Document exists so check if info from document and host data is the same
            doc_info = doc.to_dict()
            keys = ["hostGroups", "bssGlobalCoverage", "bssHwFamily", "bssLifecycleStatus"]

            doc_info_parsed = {k: doc_info[k] for k in keys}
            host_parsed = {k: host[k] for k in keys}

            if doc_info_parsed == host_parsed:
                # Both items are the same thus the feature doesn't have to be updated.
                logging.info(f"Feature with id {host['id']} was already added")
            else:
                # The data is not the same so the feature has to be updated.
                attributes = {}
                for key in keys:
                    if doc_info[key] != host[key]:
                        attributes[key] = host[key]

                ref.update(
                    attributes
                )

                arcgis_updates = {
                    "objectid": doc_info["object_id"],
                    "host_groups": host["hostGroups"],
                    "globalcoverage": host["bssGlobalCoverage"],
                    "hwfamily": host["bssHwFamily"],
                    "lifecyclestatus": host["bssLifecycleStatus"]
                }

                res = update_feature(
                    doc_info["longitude"],
                    doc_info["latitude"],
                    arcgis_updates,
                    config.LAYER["hosts"]
                )

                if res[0]["success"]:
                    logging.info(f"Succesfully updated feature with object_id: {doc_info['object_id']}")
                else:
                    logging.info(f"Failed to update feature with object_id: {doc_info['object_id']}")
                    logging.error(f"Error: {res[0]['error']}")


def do_event(data):
    for event in data["ns_tcc_events"]:
        # Make unique identifier
        unique_id = event["siteName"] + "_" + event["hostName"] + "_" + event["serviceDescription"]

        host_ref = db.collection("hosts").document(unique_id)
        host_doc = host_ref.get()

        if not host_doc.exists:
            logging.error(f"Trying to make event feature but no host info found with id: {unique_id}")
            continue

        host_info = host_doc.to_dict()

        # Check if there's already a feature of this event on ArcGIS (firestore)
        event_ref = db.collection("events").document(unique_id)
        event_doc = event_ref.get()

        attributes = {
            "id": event["id"],
            "sitename": event["siteName"],
            "type": event["type"],
            "hostname": event["hostName"],
            "servicedescription": event["serviceDescription"],
            "statetype": event["stateType"],
            "output": event["output"],
            "longoutput": event["longOutput"],
            "eventstate": event["eventState"],
            "timestamp": event["timestamp"]
        }

        if event_doc.exists:
            # There is already a feature. Check if statetype changed and delete and recreate feature if needed.
            if event["stateType"] != event_doc["stateType"]:
                # Statetype isn't the same so old feature has to be deleted and new feature has to be created.
                delete_feature(event_doc["object_id"], config.LAYER[event_doc["stateType"]])

                object_id = add_feature(
                    event["longitude"],
                    event["latitude"],
                    attributes,
                    config.LAYER[event["stateType"]]
                )

                # Update firestore with the new values of the attributes
                event_ref.update({
                    "object_id": object_id,
                    "stateType": event["stateType"]
                })
        else:
            # There is no feature yet so create new feature and add information to firestore
            object_id = add_feature(
                host_info["longitude"],
                host_info["latitude"],
                attributes,
                config.LAYER[event["stateType"]]
            )

            event_ref.set({
                "event_id": event["id"],
                "object_id": object_id,
                "stateType": event["stateType"]
            })


def main(request):
    try:
        envelope = json.loads(request.data.decode("utf-8"))
        bytes = base64.b64decode(envelope["message"]["data"])
        data = json.loads(bytes)
        subscription = envelope["subscription"].split('/')[-1]

        logging.info(f"Read message from subscription {subscription}")
    except Exception as e:
        logging.error(f"Extracting of data failed: {e}")
        return "Error", 500

    if subscription in config.SUBS["host"]:
        do_host(data)
    elif subscription in config.SUBS["host"]:
        do_event(data)
    else:
        logging.info(f"Invalid subscription received: {subscription}")

    return 'OK', 204
