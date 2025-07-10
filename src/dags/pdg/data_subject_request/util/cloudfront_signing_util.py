import base64
import datetime
import json
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from airflow.models import Variable

# This is the public key that's set up under CloudFront
key_pair_id = "K1K8VG40WCYXXQ"
expiration_time = 86400 * 30
cloudfront_base_url = "https://dqv6dwfovqcwa.cloudfront.net"


def _sign_policy(private_key, policy):
    signature = private_key.sign(policy.encode(), padding.PKCS1v15(), hashes.SHA1())
    return base64.b64encode(signature).decode()


def generate_signed_url(jira_ticket: str, file_name: str) -> str:
    private_key_str = Variable.get('dsar-cloudfront-private-key').encode("utf-8")
    private_key = serialization.load_pem_private_key(private_key_str, password=None, backend=default_backend())
    expiration_timestamp = int((datetime.datetime.utcnow() + datetime.timedelta(seconds=expiration_time)).timestamp())

    url = f'{cloudfront_base_url}/dsar-data/{jira_ticket.lower()}/{file_name}'

    policy = {"Statement": [{"Resource": url, "Condition": {"DateLessThan": {"AWS:EpochTime": expiration_timestamp}}}]}

    policy_json = json.dumps(policy)

    signature = _sign_policy(private_key, policy_json)
    return f"{url}?Policy={base64.b64encode(policy_json.encode()).decode()}&Signature={signature}&Key-Pair-Id={key_pair_id}"
