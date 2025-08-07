# Email templates:
# https://ttdcorp.sharepoint.com/:w:/s/platform-legal-files/EfH3UHYdSLVNjfofAnojPcUBFk9vLMjmLs1XQjIBQVUAiA?e=FQKnpn
# https://ttdcorp.sharepoint.com/:w:/s/platform-legal-files/EVxNzQKKNw5AqqIxECQqyokBJsEcVxNCAIcGGeUdJ_UyaA?e=eM8GGM
# https://ttdcorp.sharepoint.com/:w:/s/platform-legal-files/Efp6mtpAIt9DhiBpQ-Pg06YBxLYP3ZGiaHh82Y0PT9XKaQ?e=D260ah

from typing import Tuple

SUBJECT = "Re: Your Data Subject Rights Request"
SIGNATURE = """
<p>Regards,</p>
<p>The Trade Desk Privacy Team</p>
"""


def template_download(url: str) -> Tuple[str, str]:
    html = f"""
    <html>
        <body>
            <p>Dear Requestor,</p>
            <p>&nbsp;</p>
            <p>
            Click <a href="{url}">click here</a> to download the response to your access request.
            The link will expire within 30 days.
            You will receive the password to access the file in a second email from us.
            </p>
            <p>&nbsp;</p>
            <p>
            <b>Please note:</b> Certain data in your access request may have been obtained from third parties.
            In these cases, the name of the third-party provider for that data can be found in the "Provider" column of our response to your access request.
            For any questions about that data, please contact the provider directly.
            </p>
            <p>&nbsp;</p>
            {SIGNATURE}
        </body>
    </html>
    """
    return 'Re: Your Data Access Request', html


def template_password(file_name: str, password: str) -> Tuple[str, str]:
    html = f"""
    <html>
        <body>
            <p>Dear Requestor,</p>
            <p>&nbsp;</p>
            <p>
            You will be receiving a link to download your data access request response via another email.
            The file, <b>{file_name}</b>, is password protected; please enter the following password: <b>{password}</b> to access your response.
            </p>
            <p>&nbsp;</p>
            {SIGNATURE}
        </body>
    </html>
    """
    return 'Re: TTD Data Access Request-Password', html


def template_no_data() -> Tuple[str, str]:
    html = f"""<html>
    <body>
        <p>Dear Requestor,</p>
        <p>&nbsp;</p>
        <p>Thank you for your request.</p>
        <p>&nbsp;</p>
        <p>Please note that TTD conducted a search and did not find data associated with the information you provided.</p>
        <p>&nbsp;</p>
        <p>
        For general information regarding the different categories of data processed in our platform, data use cases and whom we share data with,
        please refer to our Services Privacy Policy, available at <a href="https://www.thetradedesk.com/legal/privacy">https://www.thetradedesk.com/legal/privacy</a>.
        Please note, you may exercise any other applicable privacy rights by following the instructions detailed under the "Your Rights and Choices" section of the Privacy Policy,
        including your rights to opt out of interest based (tailored) advertising.
        </p>
        <p>&nbsp;</p>
        <p>
        Should you have any questions related to this request or our privacy practices, please feel free to reply to this email.
        </p>
        <p>&nbsp;</p>
        {SIGNATURE}
    </body>
    </html>
    """
    return SUBJECT, html


def template_delete_confirmation() -> Tuple[str, str]:
    html = f"""<html>
    <body>
        <p>Dear Requestor,</p>
        <p>&nbsp;</p>
        <p>This message confirms that we have processed your request to delete personal information associated with the information you provided.</p>
        <p>&nbsp;</p>
        <p>Please note, we maintain limited information pursuant to applicable privacy laws, such as a record of your request in our systems.</p>
        <p>&nbsp;</p>
        {SIGNATURE}
    </body>
    </html>
    """
    return SUBJECT, html
