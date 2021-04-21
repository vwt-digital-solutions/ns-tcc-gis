import json
import logging
import os

import config
import requests
from google.cloud import secretmanager_v1


def get_secret_token():
    """
    Get Access Token from Secret Manager

    :return: Secret Manager secret
    :rtype: str
    """

    secret_client = secretmanager_v1.SecretManagerServiceClient()

    secret_name = secret_client.secret_version_path(
        os.environ["PROJECT_ID"], os.environ["SECRET_NAME"], "latest"
    )

    response = secret_client.access_secret_version(secret_name)
    secret = response.payload.data.decode("UTF-8")

    return secret


def get_arcgis_token(secret):
    """
    Get ArcGIS access token

    :param secret: ArcGIS secret
    :type secret: str

    :return: ArcGIS access token
    :rtype: str
    """

    data = {
        "f": "json",
        "username": config.CLIENT_USERNAME,
        "password": secret,
        "request": "gettoken",
        "referer": config.CLIENT_REFERER,
    }

    gis_r = None

    try:
        gis_r = requests.post(config.OAUTH_URL, data=data)
        gis_r.raise_for_status()

        r_json = gis_r.json()

        if "token" in r_json:
            return r_json["token"]

        logging.error(
            f"An error occurred when retrieving ArcGIS token: {r_json.get('error', gis_r.content)}"
        )
        return None
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
        json.decoder.JSONDecodeError,
    ) as e:
        logging.error(
            f"An error occurred when retrieving ArcGIS token: {str(e)} ({gis_r.content})"
        )
        return None
