import base64
import json
import logging
import os

import config
import requests
import zulu
from google.cloud import firestore_v1, secretmanager_v1

db = firestore_v1.Client()


def get_access_token():
    secret_client = secretmanager_v1.SecretManagerServiceClient()

    secret_name = secret_client.secret_version_path(
        os.environ["PROJECT_ID"], os.environ["SECRET_NAME"], "latest"
    )

    response = secret_client.access_secret_version(secret_name)
    secret = response.payload.data.decode("UTF-8")

    data = {
        "f": "json",
        "username": config.CLIENT_USERNAME,
        "password": secret,
        "request": "gettoken",
        "referer": config.CLIENT_REFERER,
    }

    response = requests.post(config.OAUTH_URL, data=data).json()

    return response["token"]


def add_feature(x, y, attributes, layer):
    adds = [
        {
            "geometry": {
                "x": float(x),
                "y": float(y),
                "spatalReference": {"wkid": 4326},
            },
            "attributes": attributes,
        }
    ]

    res = arcgis_feature("adds", adds, layer)

    return res["addResults"][0]


def update_feature(x, y, attributes, layer):
    updates = [{"geometry": {"x": float(x), "y": float(y)}, "attributes": attributes}]

    res = arcgis_feature("updates", updates, layer)

    return res["updateResults"][0]


def delete_feature(object_id, layer):
    data = [object_id]

    res = arcgis_feature("deletes", data, layer)

    return res["deleteResults"][0]


def arcgis_feature(function, data, layer):
    data = {function: str(data), "f": "json", "token": get_access_token()}

    r = requests.post(config.SERVICE_URL + f"/{layer}/applyEdits", data=data)

    try:
        return r.json()
    except json.decoder.JSONDecodeError as e:
        logging.error(f"Status-code: {r.status_code}")
        logging.error(f"An error occurred when applying edits: {str(e)}")


def do_host(data):
    for host in data["ns_tcc_hosts"]:
        try:
            # Check if host is already posted on ArcGIS
            host_ref = db.collection("hosts").document(host["id"])
            host_doc = host_ref.get()

            try:
                try:
                    bssglobalcoverage = host["bss_global_coverage"]["realvalue"]
                    bsshwfamily = host["bss_hw_family"]["realvalue"]
                    bsslifecyclestatus = host["bss_lifecycle_status"]["realvalue"]
                except KeyError:
                    bssglobalcoverage = host["bss_global_coverage"]["value"]
                    bsshwfamily = host["bss_hw_family"]["value"]
                    bsslifecyclestatus = host["bss_lifecycle_status"]["value"]

                host = {
                    "id": host["id"],
                    "sitename": host["sitename"],
                    "hostname": host["hostname"],
                    "decommissioned": host["decommissioned"],
                    "hostgroups": host["host_groups"],
                    "bssglobalcoverage": bssglobalcoverage,
                    "bsshwfamily": bsshwfamily,
                    "bsslifecyclestatus": bsslifecyclestatus,
                    "status": 0,  # OK
                    "giskleur": 0,  # GREEN
                    "type": "HOST",
                    "event_output": "Initial display - NS-TCC-GIS",
                    "starttime": zulu.parse(host["timestamp"]).timestamp() * 1000,
                    "longitude": host["longitude"]["value"],
                    "latitude": host["latitude"]["value"],
                }

                if host["longitude"] is None or host["latitude"] is None:
                    raise ValueError

            except (TypeError, ValueError, KeyError):
                logging.info(f"Invalid host feature data for host: {host}")
                continue

            if not host_doc.exists:
                # If host is not posted then make new feature on ArcGIS and save the ObjectID in the firestore

                response = add_feature(
                    host["longitude"], host["latitude"], host, config.LAYER["hosts"]
                )

                if response["success"]:
                    logging.info(
                        f"Successfully added '{host['id']}' as feature with objectId: {response['objectId']}"
                    )

                    host["objectId"] = response["objectId"]

                    host_ref.set(host)
                else:
                    logging.error(f"Error while adding new host: {response['error']}")
            else:
                # Document exists so check if info from document and host data is the same
                host_info = host_doc.to_dict()

                # Check if host is decommissioned and then update
                if host["decommissioned"]:
                    host_ref.set({"endtime": host["timestamp"]}, merge=True)

                    arcgis_updates = {
                        "objectid": host_info["objectId"],
                        "endtime": zulu.parse(host["timestamp"]).timestamp() * 1000,
                    }

                    response = update_feature(
                        host_info["longitude"],
                        host_info["latitude"],
                        arcgis_updates,
                        config.LAYER["hosts"],
                    )

                    if response["success"]:
                        logging.info(
                            f"Successfully updated decommissioned host: {host['id']}"
                        )
                    else:
                        logging.error(
                            f"Failed updating decommissioned host: {response['error']}"
                        )
                        continue
                    continue
                else:
                    keys = [
                        "hostgroups",
                        "bssglobalcoverage",
                        "bsshwfamily",
                        "bsslifecyclestatus",
                    ]

                    doc_info_parsed = {k: host_info[k] for k in keys}
                    host_parsed = {k: host[k] for k in keys}

                    if doc_info_parsed == host_parsed:
                        logging.info(f"Host with id {host['id']} was already added")
                    else:
                        # The data is not the same so the feature has to be updated.
                        attributes = {}
                        for key in keys:
                            if host_info[key] != host[key]:
                                attributes[key] = host[key]

                        host_ref.update(attributes)

                        arcgis_updates = {
                            "objectid": host_info["objectId"],
                            "hostgroups": host["hostgroups"],
                            "bssglobalcoverage": host["bssglobalcoverage"],
                            "bsshwfamily": host["bsshwfamily"],
                            "bsslifecyclestatus": host["bsslifecyclestatus"],
                        }

                        response = update_feature(
                            host_info["longitude"],
                            host_info["latitude"],
                            arcgis_updates,
                            config.LAYER["hosts"],
                        )

                        if response["success"]:
                            logging.info(
                                f"Successfully updated feature with objectId: {host_info['objectId']}"
                            )
                        else:
                            logging.error(
                                f"Failed to update feature: {response['error']}"
                            )
        except Exception as e:
            logging.exception(f"Error when processing host '{host['id']}': {e}")


def do_event(data):
    for event in data["ns_tcc_events"]:
        try:
            # Make unique identifier
            unique_id_host = event["sitename"] + "_" + event["hostname"]
            unique_id_event = (
                event["sitename"]
                + "_"
                + event["hostname"]
                + "_"
                + event["service_description"]
            )

            host_ref = db.collection("hosts").document(unique_id_host)
            host_doc = host_ref.get()

            event_ref = db.collection("events").document(
                unique_id_event.replace("/", "")
            )
            event_doc = event_ref.get()

            # Check if host exists
            if not host_doc.exists:
                logging.info(
                    f"Trying to update host feature but no host info found with id: {unique_id_host}"
                )
                continue

            host_info = host_doc.to_dict()

            try:
                converted_time = zulu.parse(event["timestamp"]).timestamp() * 1000
                attributes = {
                    "id": event["id"],
                    "sitename": event["sitename"],
                    "type": event["type"],
                    "hostname": event["hostname"],
                    "servicedescription": event["service_description"],
                    "statetype": event["state_type"],
                    "output": event["output"],
                    "longoutput": event["long_output"],
                    "eventstate": event["event_state"],
                    "timestamp": converted_time,
                }
            except (ValueError, KeyError):
                logging.info(f"Invalid event feature data for event: {event}")
                continue

            # Check if event exists and update firestore
            if event_doc.exists:
                if event["event_state"] != event_doc.to_dict()["eventstate"]:
                    event_ref.update(attributes)
            else:
                event_ref.set(attributes)

            # Get current "worst" states from all events of host
            event_docs = (
                db.collection("events")
                .where("sitename", "==", event["sitename"])
                .where("hostname", "==", event["hostname"])
                .stream()
            )

            host_status = 0
            event_status = 0
            host_event_output = ""
            service_event_output = ""
            for doc in event_docs:
                event_info = doc.to_dict()

                if event_info["servicedescription"] == "":
                    host_status = event_info["eventstate"]
                    host_event_output = event_info["output"]
                    continue

                if event_info["eventstate"] > event_status:
                    service_event_output = event_info["output"]
                    event_status = event_info["eventstate"]

            # Decide priority here...
            if host_status == 1 or host_status == 2 or event_status == 0:
                status = host_status
                event_type = "HOST"
                output = host_event_output
            else:  # Service state is the most critical state
                status = event_status
                event_type = "SERVICE"
                output = service_event_output

            if host_info["status"] != status or host_info["type"] != event_type:
                # Update old host feature
                arcgis_updates = {
                    "objectid": host_info["objectId"],
                    "endtime": zulu.parse(event["timestamp"]).timestamp() * 1000,
                }

                response = update_feature(
                    host_info["longitude"],
                    host_info["latitude"],
                    arcgis_updates,
                    config.LAYER["hosts"],
                )

                if response["success"]:
                    # Add new host feature
                    attributes = {
                        "sitename": event["sitename"],
                        "hostname": event["hostname"],
                        "hostgroups": host_info["hostgroups"],
                        "bssglobalcoverage": host_info["bssglobalcoverage"],
                        "bsshwfamily": host_info["bsshwfamily"],
                        "bsslifecyclestatus": host_info["bsslifecyclestatus"],
                        "giskleur": status
                        if event_type == "HOST"
                        else (status + 9),  # For colouring in GIS
                        "status": status,
                        "type": event_type,
                        "event_output": output,
                        "starttime": zulu.parse(event["timestamp"]).timestamp() * 1000,
                    }

                    response = add_feature(
                        host_info["longitude"],
                        host_info["latitude"],
                        attributes,
                        config.LAYER["hosts"],
                    )

                    if response["success"]:
                        host_ref.update(
                            {
                                "objectId": response["objectId"],
                                "status": status,
                                "type": event_type,
                                "event_output": output,
                                "starttime": zulu.parse(event["timestamp"]).timestamp()
                                * 1000,
                            }
                        )
                        logging.info(
                            f"Successfully updated host feature with event id: {unique_id_event}"
                        )
                    else:
                        logging.error(
                            f"Error when adding host feature for event: {response['error']}"
                        )
                else:
                    logging.error(
                        f"Error when updating host feature for event: {response['error']}"
                    )
            else:
                logging.info(
                    f"Received event but host feature not updated. No new status for event: {unique_id_event}"
                )
        except Exception as e:
            logging.exception(f"Error when processing event: {event['id']}: {e}")


def main(request):
    try:
        envelope = json.loads(request.data.decode("utf-8"))
        decoded = base64.b64decode(envelope["message"]["data"])
        data = json.loads(decoded)
        subscription = envelope["subscription"].split("/")[-1]

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

    return "OK", 204
