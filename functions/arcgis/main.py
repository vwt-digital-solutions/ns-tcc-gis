import base64
import json
import logging
import zulu
import requests
import os

import config

from google.cloud import firestore_v1
from google.cloud import secretmanager_v1

db = firestore_v1.Client()


def get_access_token():
    secret_client = secretmanager_v1.SecretManagerServiceClient()

    secret_name = secret_client.secret_version_path(
        os.environ["PROJECT_ID"],
        os.environ["SECRET_NAME"],
        "latest"
    )

    response = secret_client.access_secret_version(secret_name)
    secret = response.payload.data.decode("UTF-8")

    data = {
        "f": "json",
        "username": config.CLIENT_USERNAME,
        "password": secret,
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

    return res["addResults"][0]


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

    return res["updateResults"][0]


def delete_feature(object_id, layer):
    data = [
        object_id
    ]

    res = arcgis_feature("deletes", data, layer)

    return res["deleteResults"][0]


def arcgis_feature(function, data, layer):
    data = {
        function: str(data),
        "f": "json",
        "token": get_access_token()
    }

    return requests.post(config.SERVICE_URL + f"/{layer}/applyEdits", data=data).json()


def do_host(data):
    for host in data["ns_tcc_hosts"]:
        try:
            # Check if host is already posted on ArcGIS
            host_ref = db.collection("hosts").document(host["id"])
            host_doc = host_ref.get()

            if not host_doc.exists:
                # If host is not posted then make new feature on ArcGIS and save the ObjectID in the firestore
                try:
                    attributes = {
                        "sitename": host["siteName"],
                        "hostname": host["hostName"],
                        "hostgroups": host["hostGroups"],
                        "bssglobalcoverage": host["bssGlobalCoverage"],
                        "bsshwfamily": host["bssHwFamily"],
                        "bsslifecyclestatus": host["bssLifecycleStatus"],
                        "starttime": zulu.parse(host["timestamp"]).timestamp() * 1000
                    }

                    response = add_feature(
                        host["longitude"],
                        host["latitude"],
                        attributes,
                        config.LAYER["hosts"]
                    )

                except (TypeError, ValueError):
                    logging.info(f"Invalid host feature data for host: {host}")
                    continue

                if response["success"]:
                    logging.info(f"Successfully added '{host['id']}' as feature with objectId: {response['objectId']}")

                    host_ref.set({
                        "objectId": response['objectId'],
                        "siteName": host["siteName"],
                        "hostGroups": host["hostGroups"],
                        "bssGlobalCoverage": host["bssGlobalCoverage"],
                        "bssHwFamily": host["bssHwFamily"],
                        "bssLifecycleStatus": host["bssLifecycleStatus"],
                        "longitude": host["longitude"],
                        "latitude": host["latitude"],
                        "starttime": host["timestamp"]
                    })
                else:
                    logging.error(f"Error while adding new host: {response['error']}")
            else:
                # Document exists so check if info from document and host data is the same
                host_info = host_doc.to_dict()

                # Check if host is decommissioned and then update
                if host["decommissioned"]:
                    host_ref.set({
                        "endtime": host["timestamp"]
                    }, merge=True)

                    arcgis_updates = {
                        "objectid": host_info["objectId"],
                        "endtime": zulu.parse(host["timestamp"]).timestamp() * 1000
                    }

                    response = update_feature(
                        host_info["longitude"],
                        host_info["latitude"],
                        arcgis_updates,
                        config.LAYER["hosts"]
                    )

                    if response["success"]:
                        logging.info(f"Successfully updated decommissioned host: {host['id']}")
                    else:
                        logging.error(f"Failed updating decommissioned host: {response['error']}")
                        continue

                    # Check for the events created by this host
                    event_docs = db.collection("events") \
                        .where("sitename", "==", host["siteName"]) \
                        .where("hostname", "==", host["hostName"]) \
                        .stream()

                    for event_doc in event_docs:
                        event_info = event_doc.to_dict()

                        arcgis_updates = {
                            "objectid": event_info["objectId"],
                            "endtime": zulu.parse(host["timestamp"]).timestamp() * 1000
                        }

                        response = update_feature(
                            host_info["longitude"],
                            host_info["latitude"],
                            arcgis_updates,
                            config.LAYER[event_info["statetype"]]
                        )

                        if response["success"]:
                            event_doc.reference.set({
                                "endtime": host["timestamp"]
                            }, merge=True)
                            logging.info(f"Successfully updated event of decommissioned host with eventId: "
                                         f"{host_doc.id}")
                        else:
                            logging.error(f"Failed to update event of decommissioned host: {response['error']}")
                    continue
                else:
                    keys = ["hostGroups", "bssGlobalCoverage", "bssHwFamily", "bssLifecycleStatus"]

                    doc_info_parsed = {k: host_info[k] for k in keys}
                    host_parsed = {k: host[k] for k in keys}

                    # Check if hosts are the same and if it's been decommissioned before (endtime)
                    if doc_info_parsed == host_parsed:
                        logging.info(f"Host with id {host['id']} was already added")
                    else:
                        # The data is not the same so the feature has to be updated.
                        attributes = {}
                        for key in keys:
                            if host_info[key] != host[key]:
                                attributes[key] = host[key]

                        host_ref.update(
                            attributes
                        )

                        arcgis_updates = {
                            "objectid": host_info["objectId"],
                            "host_groups": host["hostGroups"],
                            "globalcoverage": host["bssGlobalCoverage"],
                            "hwfamily": host["bssHwFamily"],
                            "lifecyclestatus": host["bssLifecycleStatus"]
                        }

                        response = update_feature(
                            host_info["longitude"],
                            host_info["latitude"],
                            arcgis_updates,
                            config.LAYER["hosts"]
                        )

                        if response["success"]:
                            logging.info(f"Successfully updated feature with objectId: {host_info['objectId']}")
                        else:
                            logging.error(f"Failed to update feature: {response['error']}")
        except Exception as e:
            logging.error(f"Error when processing host '{host['id']}': {e}")
            logging.exception(e)


def do_event(data):
    for event in data["ns_tcc_events"]:
        try:
            # Make unique identifier
            unique_id_host = event["siteName"] + "_" + event["hostName"]
            unique_id_event = event["siteName"] + "_" + event["hostName"] + "_" + event["serviceDescription"]

            host_ref = db.collection("hosts").document(unique_id_host)
            host_doc = host_ref.get()

            if not host_doc.exists:
                logging.error(f"Trying to make event feature but no host info found with id: {unique_id_host}")
                continue

            host_info = host_doc.to_dict()

            # Check if there's already a feature of this event on ArcGIS (firestore)
            event_ref = db.collection("events").document(unique_id_event)
            event_doc = event_ref.get()

            converted_time = zulu.parse(event["timestamp"]).timestamp() * 1000
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
                "timestamp": converted_time,
                "starttime": converted_time
            }

            if event_doc.exists:
                # There is already a feature. Check if statetype changed and delete and recreate feature if needed.
                event_info = event_doc.to_dict()

                if event["stateType"] != event_info["statetype"]:
                    # Statetype isn't the same so old feature has to be deleted and new feature has to be created.
                    arcgis_updates = {
                        "objectid": event_info["objectId"],
                        "endtime": zulu.parse(event["timestamp"]).timestamp() * 1000
                    }

                    update_response = update_feature(
                        host_info["longitude"],
                        host_info["latitude"],
                        arcgis_updates,
                        config.LAYER[event_info["statetype"]]
                    )

                    if update_response["success"]:
                        response = add_feature(
                            host_info["longitude"],
                            host_info["latitude"],
                            attributes,
                            config.LAYER[event["stateType"]]
                        )

                        if response["success"]:
                            # Update firestore with the new values of the attributes
                            attributes["timestamp"] = event["timestamp"]
                            attributes["objectId"] = response["objectId"]
                            event_ref.update(attributes)

                            logging.info(f"Event feature '{unique_id_event}'with objectId: {response['objectId']} "
                                         f"updated.")
                        else:
                            logging.error(f"Failed changing event feature: {response['error']}")
                    else:
                        logging.error(f"Failed deleting event feature: {update_response['error']}")
                else:
                    logging.info(f"Event with id: '{unique_id_event}' already has a feature and is unchanged.")
            else:
                # There is no feature yet so create new feature and add information to firestore
                response = add_feature(
                    host_info["longitude"],
                    host_info["latitude"],
                    attributes,
                    config.LAYER[event["stateType"]]
                )

                if response["success"]:
                    attributes["timestamp"] = event["timestamp"]
                    attributes["objectId"] = response["objectId"]
                    event_ref.set(attributes)

                    logging.info(
                        f"Added new feature of event '{unique_id_event}' with objectId: {response['objectId']}")
                else:
                    logging.error(f"Failed adding new event feature: {response['error']}")
        except Exception as e:
            logging.error(f"Error when processing event: {event['id']}")
            logging.exception(e)


def main(request):
    try:
        envelope = json.loads(request.data.decode("utf-8"))
        decoded = base64.b64decode(envelope["message"]["data"])
        data = json.loads(decoded)
        subscription = envelope["subscription"].split('/')[-1]

        logging.info(f"Read message from subscription {subscription}")
    except Exception as e:
        logging.error(f"Extracting of data failed: {e}")
        return "Error", 500

    if subscription == config.SUBS["host"]:
        do_host(data)
    elif subscription == config.SUBS["event"]:
        do_event(data)
    else:
        logging.info(f"Invalid subscription received: {subscription}")

    return 'OK', 204
