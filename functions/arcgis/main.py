import base64
import json
import logging

import config
import requests
import secretmanager
import zulu
from google.cloud import firestore_v1

db_client = firestore_v1.Client()
arcgis_access_token = secretmanager.get_access_token()


class ArcGISProcessor:
    def __init__(self):
        pass

    @staticmethod
    def apply_edits(function, data, layer):
        """
        Apply ArcGIS edits

        :param function: Function
        :param data: ArcGIS data
        :param layer: ArcGIS layer
        """

        data = {function: str(data), "f": "json", "token": arcgis_access_token}

        r = requests.post(config.SERVICE_URL + f"/{layer}/applyEdits", data=data)

        try:
            response_json = r.json()
        except json.decoder.JSONDecodeError as e:
            logging.error(
                f"An error occurred when applying edits (status-code: {r.status_code}): {str(e)}"
            )
            return None
        else:
            return response_json

    def add_feature(self, x, y, attributes, layer):
        """
        Add ArcGIS Feature

        :param x: X-coordinate
        :param y: Y-coordinate
        :param attributes: Feature attributes
        :param layer: Feature layer

        :return: ArcGIS feature ID
        """

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

        res = self.apply_edits("adds", adds, layer)

        if res and "addResults" in res:
            return res["addResults"][0]

        return None

    def update_feature(self, x, y, attributes, layer):
        """
        Update ArcGIS Feature

        :param x: X-coordinate
        :param y: Y-coordinate
        :param attributes: Feature attributes
        :param layer: Feature layer

        :return: ArcGIS feature ID
        """

        updates = [
            {"geometry": {"x": float(x), "y": float(y)}, "attributes": attributes}
        ]

        res = self.apply_edits("updates", updates, layer)

        if res and "updateResults" in res:
            return res["updateResults"][0]

        return None

    def delete_feature(self, object_id, layer):
        """
        Delete ArcGIS feature

        :param object_id: Feature ID
        :param layer: Feature layer

        :return: ArcGIS feature ID
        """

        data = [object_id]

        res = self.apply_edits("deletes", data, layer)

        if res and "deleteResults" in res:
            return res["deleteResults"][0]

        return None


class HostProcessor:
    def __init__(self, arcgis_processor):
        self.arcgis_processor = arcgis_processor

    def process(self, host):
        """
        Process each host data

        :param host: Host data
        """

        try:
            # Check if host is already posted on ArcGIS
            host_ref = db_client.collection("hosts").document(host["id"])
            host_doc = host_ref.get()

            host_formatted = self.get_host_object(host)  # Get formatted host object

            if not host_formatted:
                return

            if not host_doc.exists:
                self.add_new_host(host_formatted, host_ref)
            else:
                self.update_existing_host(host_formatted, host_doc, host_ref)
        except Exception as e:
            logging.exception(f"Error when processing host '{host['id']}': {e}")

    def update_existing_host(self, host, host_doc, host_ref):
        """
        Check and update existing host

        :param host: Host data
        :param host_doc: Host Firestore document
        :param host_ref: Host Firestore reference
        """

        # Document exists so check if info from document and host data is the same
        host_info = host_doc.to_dict()

        # Check if host is decommissioned and then update
        if host["decommissioned"]:
            self.update_existing_decommissioned_host(host, host_info, host_ref)
        else:
            self.update_existing_active_host(host, host_info, host_ref)

    def update_existing_active_host(self, host, host_info, host_ref):
        """
        Update existing active host

        :param host: Host data
        :param host_info: Host information
        :param host_ref: Host Firestore reference
        """

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
            return

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

        response = self.arcgis_processor.update_feature(
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
            logging.error(f"Failed to update feature: {response['error']}")

    def update_existing_decommissioned_host(self, host, host_info, host_ref):
        """
        Update decommissioned host

        :param host: Host data
        :param host_info: Host information
        :param host_ref: Host Firestore reference
        """

        host_ref.set({"endtime": host["timestamp"]}, merge=True)
        arcgis_updates = {
            "objectid": host_info["objectId"],
            "endtime": zulu.parse(host["timestamp"]).timestamp() * 1000,
        }

        response = self.arcgis_processor.update_feature(
            host_info["longitude"],
            host_info["latitude"],
            arcgis_updates,
            config.LAYER["hosts"],
        )

        if response["success"]:
            logging.info(f"Successfully updated decommissioned host: {host['id']}")
        else:
            logging.error(f"Failed updating decommissioned host: {response['error']}")

    def add_new_host(self, host, host_ref):
        """
        Add new host to ArcGIS and Firestore

        :param host: Host data
        :param host_ref: Host Firestore reference
        """

        response = self.arcgis_processor.add_feature(
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

    def get_host_object(self, host):
        """
        Get host object

        :param host: Host data

        :return: Host data
        """
        try:
            bssglobalcoverage, bsshwfamily, bsslifecyclestatus = self.get_bss_variables(
                host
            )
            start_time = zulu.parse(host["timestamp"]).timestamp() * 1000

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
                "starttime": start_time,
                "longitude": host["longitude"]["value"],
                "latitude": host["latitude"]["value"],
            }

            if host["longitude"] is None or host["latitude"] is None:
                raise ValueError

        except (TypeError, ValueError, KeyError) as e:
            logging.info(f"Invalid host feature data for host: {host}: {str(e)}")
            return None
        else:
            return host

    @staticmethod
    def get_bss_variables(host):
        """
        Get BSS variables from host data

        :param host: Host data

        :return: BSS global coverage, BSS HW family, BSS life cycle status
        """

        try:
            bssglobalcoverage = host["bss_global_coverage"]["realvalue"]
            bsshwfamily = host["bss_hw_family"]["realvalue"]
            bsslifecyclestatus = host["bss_lifecycle_status"]["realvalue"]
        except KeyError:
            bssglobalcoverage = host["bss_global_coverage"]["value"]
            bsshwfamily = host["bss_hw_family"]["value"]
            bsslifecyclestatus = host["bss_lifecycle_status"]["value"]

        return bssglobalcoverage, bsshwfamily, bsslifecyclestatus


class EventProcessor:
    def __init__(self, arcgis_processor):
        self.arcgis_processor = arcgis_processor

    def process(self, event):
        """
        Process each event data

        :param event: Event data
        """

        try:
            unique_id_event, unique_id_host = self.make_unique_identifier(event)

            host_ref = db_client.collection("hosts").document(unique_id_host)
            host_doc = host_ref.get()

            event_ref = db_client.collection("events").document(
                unique_id_event.replace("/", "")
            )
            event_doc = event_ref.get()

            # Check if host exists
            if not host_doc.exists:
                logging.info(
                    f"Trying to update host feature but no host info found with id: {unique_id_host}"
                )
                return

            host_info = host_doc.to_dict()

            attributes = self.get_attributes(event)
            if not attributes:
                return

            # Check if event exists and update firestore
            if event_doc.exists:
                if event["event_state"] != event_doc.to_dict()["eventstate"]:
                    event_ref.update(attributes)

            if not event_doc.exists:
                event_ref.set(attributes)

            # Get current "worst" states from all events of host
            (
                event_status,
                host_event_output,
                host_status,
                service_event_output,
            ) = self.get_worst_states_of_host(event)

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
                self.update_host_status(
                    event,
                    event_type,
                    host_info,
                    host_ref,
                    output,
                    status,
                    unique_id_event,
                )
            else:
                logging.info(
                    f"Received event but host feature not updated. No new status for event: {unique_id_event}"
                )
        except Exception as e:
            logging.exception(f"Error when processing event: {event['id']}: {e}")

    def update_host_status(
        self, event, event_type, host_info, host_ref, output, status, unique_id_event
    ):
        """
        Update host to new status

        :param event: Event data
        :param event_type: Event type
        :param host_info: Host information
        :param host_ref: Host reference
        :param output: Output
        :param status: Status
        :param unique_id_event: Unique ID event
        """

        # Update old host feature
        arcgis_updates = {
            "objectid": host_info["objectId"],
            "endtime": zulu.parse(event["timestamp"]).timestamp() * 1000,
        }
        response = self.arcgis_processor.update_feature(
            host_info["longitude"],
            host_info["latitude"],
            arcgis_updates,
            config.LAYER["hosts"],
        )

        if response["success"]:
            gis_kleur = (
                status if event_type == "HOST" else (status + 9)
            )  # For colouring in GIS
            start_time = (
                zulu.parse(event["timestamp"]).timestamp() * 1000
            )  # Format timestamp

            # Add new host feature
            attributes = {
                "sitename": event["sitename"],
                "hostname": event["hostname"],
                "hostgroups": host_info["hostgroups"],
                "bssglobalcoverage": host_info["bssglobalcoverage"],
                "bsshwfamily": host_info["bsshwfamily"],
                "bsslifecyclestatus": host_info["bsslifecyclestatus"],
                "giskleur": gis_kleur,
                "status": status,
                "type": event_type,
                "event_output": output,
                "starttime": start_time,
            }

            response = self.arcgis_processor.add_feature(
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
                        "starttime": zulu.parse(event["timestamp"]).timestamp() * 1000,
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

    @staticmethod
    def get_worst_states_of_host(event):
        """
        Return current "worst" states from all events of host

        :param event: Event data

        :return: Event status, Host event output, Host status, Service event output
        """

        host_status = 0
        event_status = 0
        host_event_output = ""
        service_event_output = ""

        event_docs = (
            db_client.collection("events")
            .where("sitename", "==", event["sitename"])
            .where("hostname", "==", event["hostname"])
            .stream()
        )

        for doc in event_docs:
            event_info = doc.to_dict()

            if event_info["servicedescription"] == "":
                host_status = event_info["eventstate"]
                host_event_output = event_info["output"]
                continue

            if event_info["eventstate"] > event_status:
                service_event_output = event_info["output"]
                event_status = event_info["eventstate"]

        return event_status, host_event_output, host_status, service_event_output

    @staticmethod
    def get_attributes(event):
        """
        Return event attributes

        :param event: Event Data

        :return: Event attributes
        """

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
            return None
        else:
            return attributes

    @staticmethod
    def make_unique_identifier(event):
        """
        Make unique identifier

        :param event: Event Data

        :return: Unique event ID, Unique host ID
        """

        unique_id_host = f"{event['sitename']}_{event['hostname']}"
        unique_id_event = (
            f"{event['sitename']}_{event['hostname']}_{event['service_description']}"
        )

        return unique_id_event, unique_id_host


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

    arcgis_processor = ArcGISProcessor()

    if subscription == config.SUBS["host"]:
        host_processor = HostProcessor(arcgis_processor=arcgis_processor)

        for host in data["ns_tcc_hosts"]:
            host_processor.process(host)
    elif subscription == config.SUBS["event"]:
        event_processor = EventProcessor(arcgis_processor=arcgis_processor)

        for event in data["ns_tcc_events"]:
            event_processor.process(event)
    else:
        logging.info(f"Invalid subscription received: {subscription}")

    return "OK", 204
