import os
import pathlib

_SECRETS_DIR = str(pathlib.Path(__file__).parents[1] / ".secrets")


def get_kafka_ssl_config() -> dict:
    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SSL",
        "ssl.ca.location": os.environ.get("KAFKA_SSL_CA", f"{_SECRETS_DIR}/ca.pem"),
        "ssl.certificate.location": os.environ.get(
            "KAFKA_SSL_CERT", f"{_SECRETS_DIR}/service.cert"
        ),
        "ssl.key.location": os.environ.get(
            "KAFKA_SSL_KEY", f"{_SECRETS_DIR}/service.key"
        ),
    }
