import boto3
import logging
from dags.pdg.data_subject_request.util import email_template_util
from ttd.ttdenv import TtdEnvFactory

FROM_EMAIL = 'privacy@thetradedesk.com'
is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False


def _send_email(email: str, subject: str, html_content: str):
    ses_client = boto3.client('sesv2', region_name='us-east-1')

    if not is_prod:
        logging.info(html_content)
    else:
        # Legal has asked that we copy them on all responses we send from the automated DSR processes.
        # That's why we're also adding a BCC to the privacy email below.
        ses_client.send_email(
            FromEmailAddress=FROM_EMAIL,
            Destination={
                "ToAddresses": [email],
                "BccAddresses": [FROM_EMAIL]
            },
            Content={"Simple": {
                "Subject": {
                    "Data": subject
                },
                "Body": {
                    "Html": {
                        "Data": html_content
                    },
                },
            }},
        )


def send_password_email(email: str, password: str, file_name):
    subject, html = email_template_util.template_password(file_name, password)
    _send_email(email, subject, html)


def send_download_email(email: str, url: str):
    subject, html = email_template_util.template_download(url)
    _send_email(email, subject, html)


# Sends an email to a DSAR requestor when no data was found in the platform
def send_no_data_email(email: str):
    subject, html = email_template_util.template_no_data()
    _send_email(email, subject, html)


def send_delete_confirmation(email: str):
    subject, html = email_template_util.template_delete_confirmation()
    _send_email(email, subject, html)
