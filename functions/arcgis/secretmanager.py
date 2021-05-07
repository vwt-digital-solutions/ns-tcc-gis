import os

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
