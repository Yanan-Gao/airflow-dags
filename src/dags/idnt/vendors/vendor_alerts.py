from dataclasses import dataclass
from ttd.ttdslack import get_slack_client
from datetime import datetime, date, timedelta
import logging
from dags.idnt.statics import Tags, VendorTypes
from typing import Callable
from dags.idnt.util.s3_utils import get_latest_existing_date
from typing import Optional


@dataclass
class LateVendor:
    """Contains all the information needed for a vendor which is late for its delivery.

     Attributes:
         name (str): The name of the vendor which is late.
         vendor_type (VendorTypes): The type of the vendor which is late.
         input_path (str): The input data path for the vendor which is late.
         cadence_days (int): The expected number of days between each delivery for the late vendor.
         last_delivery (date): The date when the vendor made their last delivery.
         missed_delivery (date): The date when the vendor was supposed to next deliver their data but didn't.
         days_late (int): The number of days the missed delivery is late from the current run date.
     """
    name: str
    vendor_type: VendorTypes
    input_path: str
    cadence_days: int
    last_delivery: date
    missed_delivery: date
    days_late: int

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, LateVendor):
            return self.name == other.name
        return False

    def to_dict(self):
        """Convert object to dictionary for serialization."""
        return {
            "name": self.name,
            "vendor_type": self.vendor_type,
            "input_path": self.input_path,
            "cadence_days": self.cadence_days,
            "last_delivery": self.last_delivery,
            "missed_delivery": self.missed_delivery,
            "days_late": self.days_late
        }

    @staticmethod
    def from_dict(data: dict):
        """Convert dictionary back to object."""
        return LateVendor(
            name=data["name"],
            vendor_type=data["vendor_type"],
            input_path=data["input_path"],
            cadence_days=data["cadence_days"],
            last_delivery=data["last_delivery"],
            missed_delivery=data["missed_delivery"],
            days_late=data["days_late"]
        )

    @staticmethod
    def get_latest_input_date(current_date: datetime, last_etl_date: datetime, input_path_generator: Callable[[datetime],
                                                                                                              str]) -> Optional[datetime]:
        """
        Finds the latest available input date between the last ETL run date and the current DAG run date.

        Args:
            current_date (datetime): The date when the DAG is running.
            last_etl_date (datetime): The last successful ETL run date.
            input_path_generator: Callable[[datetime], str]: The path where input data is stored.

        Returns:
            datetime or None: The most recent existing input date found, or None if no valid date is found.
        """
        # Check all dates between last etl date and dag run date
        dates_to_check = [current_date - timedelta(days=i) for i in range((current_date - last_etl_date).days)]
        latest_existing_date = get_latest_existing_date(dates_to_check, input_path_generator)

        return latest_existing_date

    @staticmethod
    def is_vendor_late(current_date: datetime, last_etl_date: datetime, cadence_days: int, buffer_days: int) -> bool:
        """
        Determines whether a vendor is late in delivering data based on the expected cadence and buffer period.

        This function calculates the number of days since the last ETL run and compares it against
        the expected delivery schedule (`cadence_days`) plus an allowed buffer (`buffer_days`).
        If the delay exceeds this threshold, the vendor is considered late.

        Args:
            current_date (datetime): The date when the check is performed.
            last_etl_date (datetime): The date of the last successful ETL run.
            cadence_days (int): The expected frequency (in days) of data delivery.
            buffer_days (int): Additional buffer days allowed before considering the vendor late.

        Returns:
            bool: True if the vendor is late, otherwise False.
        """
        num_days_since_last_etl = (current_date - last_etl_date).days
        late_threshold = cadence_days + buffer_days

        return num_days_since_last_etl > late_threshold

    @staticmethod
    def post_slack_alert(late_vendors: set, job_size):
        alert_generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        vendor_affected_names = ", ".join([f"{vendor.name}" for vendor in late_vendors])
        vendors_affected = "\n".join([f"- {vendor.name}: {vendor.days_late} day(s) late" for vendor in late_vendors])
        late_vendors_display = "\n".join(
            f"{vendor.name}  |  {vendor.vendor_type} Vendor  |  Every {vendor.cadence_days} days  |  {vendor.last_delivery}  |  {vendor.missed_delivery}  |  {vendor.days_late}  |  {vendor.input_path}"
            for vendor in late_vendors
        )

        alert_message = f"""
*Late Data Alert (Generated: {alert_generated})*


*=== Summary ===*

Total {job_size} vendors affected: {len(late_vendors)}
{vendors_affected}


*=== Details ===*

Vendor  |  Vendor Type  |  Expected Delivery Frequency  |  Last Delivery  |  Missed Delivery  |  Days Late  |  Input Path
{"-" * 110}
{late_vendors_display}


*=== Actions ===*

1. Investigate the delay by checking the input path
2. Follow up with Data Partnerships on <https://thetradedesk.enterprise.slack.com/archives/C02LER1111P|#ftr-identity-vendor-evaluation> by copy-pasting this message:

'Hi team, <Vendor> (<Input Path>) has not delivered us data within the last <Days Late> days since their expected delivery on <Missed Delivery>. Please follow up and let us know. Thanks!'"""

        logging.warning(f"Data is missing for: {vendor_affected_names}. Message posted to {Tags.slack_channel()}")
        get_slack_client().chat_postMessage(channel=Tags.slack_channel(), text=alert_message)
