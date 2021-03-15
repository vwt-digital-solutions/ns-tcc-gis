import os

import config
import requests
from google.cloud import secretmanager_v1


def get_access_token():
    """
    Get Access Token from Secret Manager

    :return: Secret Manager secret
    """

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
