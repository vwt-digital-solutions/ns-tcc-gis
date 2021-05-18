import logging
from json.decoder import JSONDecodeError

import config
import requests
from requests.exceptions import ConnectionError, HTTPError
from retry import retry


def get_feature_service_token(secret):
    """
    Request a new feature service token

    :param secret: ArcGIS secret
    :type secret: str

    :return: Token
    :rtype: str
    """

    try:
        return get_arcgis_token(secret)
    except KeyError as e:
        logging.error(
            f"Function is missing authentication configuration for retrieving ArcGIS token: {str(e)}"
        )
        return None
    except (ConnectionError, HTTPError) as e:
        logging.error(f"An error occurred when retrieving ArcGIS token: {str(e)}")
        return None
    except JSONDecodeError as e:
        logging.debug(f"An error occurred when retrieving ArcGIS token: {str(e)}")
        return None


@retry(
    (ConnectionError, HTTPError, JSONDecodeError),
    tries=3,
    delay=5,
    logger=None,
    backoff=2,
)
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

    gis_r = requests.post(config.OAUTH_URL, data=data)
    gis_r.raise_for_status()

    r_json = gis_r.json()

    if "token" in r_json:
        return r_json["token"]

    logging.error(
        f"An error occurred when retrieving ArcGIS token: {r_json.get('error', gis_r.content)}"
    )
    return None
