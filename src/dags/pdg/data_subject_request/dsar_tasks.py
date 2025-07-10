import os
import io
import logging
import csv
import secrets
from datetime import datetime
from typing import List
import pandas as pd

from ttd.tasks.op import OpTask
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.models import TaskInstance
from dags.pdg.data_subject_request.dsr_item import DsrItem
from dags.pdg.data_subject_request.util import jira_util, uid2_util, euid_util, aerospike_util, s3_utils, provdb_util, \
     ses_util, vertica_util, phone_number_util, cloudfront_signing_util, serialization_util
from dags.pdg.data_subject_request.identity import DsrIdentity, DsrIdentityType, DsrIdentitySource
import msoffcrypto


class DataSubjectAccessRequestTasks:

    FAST_CHECK_TASK_ID = "fast_check"
    RETRIEVE_COLD_STORAGE_DATA_TASK_ID = "retrieve_cold_storage_data"
    TICKETS_READY_FOR_FULFILLMENT_TASK_ID = "tickets_ready_for_fulfillment"
    RETRIEVE_TARGETING_DATA_TASK_ID = "retrieve_targeting_data"
    RETRIEVE_VERTICA_DATA_TASK_ID = "retrieve_vertica_bidfeedback_data"
    FINALIZE_OUTPUT_TASK_ID = "finalize_output"

    PASSWORD_LENGTH = 8

    JIRA_NOTIFY_USERS = ['james.abendroth@thetradedesk.com', 'brent.kobryn@thetradedesk.com']

    def __init__(self, dag_id: str):
        self.dag_id = dag_id

    @staticmethod
    def _inject_ids_from_identifiers(dsr_items: List[DsrItem]):
        if not dsr_items:
            return

        uid2_client = uid2_util.create_client()
        euid_client = euid_util.create_client()

        for dsr_item in dsr_items:
            email = dsr_item.email
            phone = dsr_item.phone

            result = uid2_util.get_uid2_and_hashed_guid(email, DsrIdentitySource.EMAIL, uid2_client)
            if result is not None:
                uid2, guid = result
                identity = DsrIdentity(DsrIdentityType.UID2, DsrIdentitySource.EMAIL, uid2, guid)
                dsr_item.add_identity(identity)

            result = euid_util.get_euid_and_hashed_guid(email, DsrIdentitySource.EMAIL, euid_client)
            if result is not None:
                euid, guid = result
                identity = DsrIdentity(DsrIdentityType.EUID, DsrIdentitySource.EMAIL, euid, guid)
                dsr_item.add_identity(identity)

            if phone:
                result = uid2_util.get_uid2_and_hashed_guid(phone, DsrIdentitySource.PHONE, uid2_client)
                if result is not None:
                    uid2, guid = result
                    identity = DsrIdentity(DsrIdentityType.UID2, DsrIdentitySource.PHONE, uid2, guid)
                    dsr_item.add_identity(identity)

                result = euid_util.get_euid_and_hashed_guid(phone, DsrIdentitySource.PHONE, euid_client)
                if result is not None:
                    euid, guid = result
                    identity = DsrIdentity(DsrIdentityType.EUID, DsrIdentitySource.PHONE, euid, guid)
                    dsr_item.add_identity(identity)

    def _retrieve_cold_storage_segments(self, task_instance: TaskInstance):
        dsr_items = jira_util.read_jira_dsr_queue(jira_util.REQUEST_TYPE_ACCESS, jira_util.jira_in_progress_status)
        if not dsr_items:
            logging.info('No In Progress tickets to pull segments for.')
            return False

        dsr_items = phone_number_util.validate_dsr_request_phones(dsr_items)
        if not dsr_items:
            logging.info('No items to process after phone validation')
            return False

        self._inject_ids_from_identifiers(dsr_items)

        aerospike_client = aerospike_util.get_aerospike_client()

        for dsr_item in dsr_items:
            jira_ticket_number = dsr_item.jira_ticket_number
            is_us = dsr_item.request_is_us

            dataframes = []
            for guid in dsr_item.guids:
                df = aerospike_util.collect_key_to_dataframe(aerospike_client, guid)
                if df is not None:
                    dataframes.append(df)

            if not dataframes:
                # Write an empty file to both track that we didn't find segments, as well
                # as so we have a file to read later on in the process when finalizing output.
                empty_segment_header = "TargetingDataId,Expiration\n"
                s3_utils.put_user_segments(empty_segment_header, jira_ticket_number)
                s3_utils.put_user_segments_backup(empty_segment_header, jira_ticket_number, is_us)
                logging.info(f'No Cold Storage data found for request {jira_ticket_number}')
                jira_util.transition_jira_status([jira_ticket_number], jira_util.jira_ready_for_fulfillment_status)
                continue

            final_df = pd.concat(dataframes, ignore_index=True)  # Concatenate all DataFrames

            csv_file = io.StringIO()
            final_df.to_csv(csv_file, index=False)

            csv_content: str = csv_file.getvalue()
            s3_utils.put_user_segments(csv_content, jira_ticket_number)
            s3_utils.put_user_segments_backup(csv_content, jira_ticket_number, is_us)

            jira_util.post_comment_to_ticket(jira_ticket_number, 'Data was found for the supplied email')
            jira_util.transition_jira_status([jira_ticket_number], jira_util.jira_ready_for_fulfillment_status)

        task_instance.xcom_push(key='dsr_items', value=serialization_util.serialize_list(dsr_items))
        return True

    def create_retrieve_cold_storage_task(self, airflow_dag):
        return OpTask(
            op=ShortCircuitOperator(
                task_id=DataSubjectAccessRequestTasks.RETRIEVE_COLD_STORAGE_DATA_TASK_ID,
                dag=airflow_dag,
                provide_context=True,
                python_callable=self._retrieve_cold_storage_segments,
                retry_exponential_backoff=False
            )
        )

    def create_targeting_data_task(self, airflow_dag):
        return OpTask(
            op=PythonOperator(
                task_id=DataSubjectAccessRequestTasks.RETRIEVE_TARGETING_DATA_TASK_ID,
                dag=airflow_dag,
                provide_context=True,
                python_callable=self._retrieve_targeting_data,
                retry_exponential_backoff=False
            )
        )

    @staticmethod
    def _retrieve_targeting_data(task_instance: TaskInstance, **kwargs):
        dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
        for dsr_item in dsr_items:
            jira_ticket = dsr_item.jira_ticket_number
            is_us = dsr_item.request_is_us
            targeting_data_ids = []

            logging.info(f'Pulling segments for request {jira_ticket}')
            cold_storage_blob = s3_utils.get_cold_storage_data(jira_ticket)
            csv_data = io.BytesIO(cold_storage_blob)
            dataframe = pd.read_csv(csv_data)

            # It's possible to have requests where no targeting data is found, but BidFeedback data is.
            # We still want to return results to requestors in this case, but the DMP spreadsheet tab
            # will be empty. We'll also write an empty file as a record that no data was found.
            if dataframe.empty:
                empty_dmp_header = "Provider,Full Path\n"
                s3_utils.put_targeting_data(empty_dmp_header, jira_ticket)
                s3_utils.put_targeting_data_backup(empty_dmp_header, jira_ticket, is_us)

                logging.info(f'Cold Storage file was empty for request {jira_ticket}')
                continue

            for index, row in dataframe.iterrows():
                targeting_data_ids.append(row['TargetingDataId'])

            data = provdb_util.query_for_additional_provisioning_data(targeting_data_ids)
            if not data:
                logging.info(f'No targeting data found for request {jira_ticket}')
                continue

            csv_file = io.StringIO()
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['Provider', 'Full Path'])

            for row in data:
                csv_writer.writerow(row)

            csv_content: str = csv_file.getvalue()
            s3_utils.put_targeting_data(csv_content, jira_ticket)
            s3_utils.put_targeting_data_backup(csv_content, jira_ticket, is_us)

        task_instance.xcom_push(key='dsr_items', value=serialization_util.serialize_list(dsr_items))
        return True

    @staticmethod
    def _retrieve_vertica_data(task_instance: TaskInstance, **kwargs):
        dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
        if not dsr_items:
            logging.info('No items to process.')
            return False

        results = vertica_util.query_bidfeedback(dsr_items)

        for dsr_item in dsr_items:
            jira_ticket_number = dsr_item.jira_ticket_number
            is_us = dsr_item.request_is_us

            # Create a CSV file in memory
            csv_file = io.StringIO()
            csv_writer = csv.writer(csv_file)

            for row in results[jira_ticket_number]:
                csv_writer.writerow(row)

            csv_content: str = csv_file.getvalue()
            s3_utils.put_bid_feedback_data(csv_content, jira_ticket_number)
            s3_utils.put_bid_feedback_data_backup(csv_content, jira_ticket_number, is_us)

        task_instance.xcom_push(key='dsr_items', value=serialization_util.serialize_list(dsr_items))
        return True

    def create_vertica_data_task(self, airflow_dag):
        return OpTask(
            op=PythonOperator(
                task_id=DataSubjectAccessRequestTasks.RETRIEVE_VERTICA_DATA_TASK_ID,
                dag=airflow_dag,
                provide_context=True,
                python_callable=self._retrieve_vertica_data,
                retry_exponential_backoff=True
            )
        )

    @staticmethod
    def _finalize_output(task_instance: TaskInstance, **kwargs):
        import pandas as pd

        dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
        if not dsr_items:
            logging.info('No items to process.')
            return False

        for dsr_item in dsr_items:
            jira_ticket = dsr_item.jira_ticket_number
            email = dsr_item.email

            targeting_data = s3_utils.get_targeting_data(jira_ticket)
            bidfeedback_data = s3_utils.get_bidfedback_data(jira_ticket)

            td_csv_data = io.BytesIO(targeting_data)
            bf_csv_data = io.BytesIO(bidfeedback_data)

            targeting_data_dataframe = pd.read_csv(td_csv_data)
            bidfeedback_data_dataframe = pd.read_csv(bf_csv_data)

            # This replaces what we used to do in the fast check. We now skip the fast check, but
            # we'll still send the "no data" email if no data has been found for the requestor.
            if targeting_data_dataframe.empty and bidfeedback_data_dataframe.empty:
                logging.info(f'Both TargetingData and BidFeedback data were empty for request {jira_ticket}')
                jira_util.transition_jira_status([jira_ticket], jira_util.jira_done_status)
                ses_util.send_no_data_email(email)
                continue

            file_name = f'{jira_ticket}_{datetime.now().strftime("%Y%m%d")}.xlsx'
            temp_path = '/tmp/tmp.xlsx'
            final_path = f'/tmp/{file_name}'
            with pd.ExcelWriter(temp_path, engine='xlsxwriter') as writer:
                targeting_data_dataframe.to_excel(writer, sheet_name='DMP Results', index=False)
                bidfeedback_data_dataframe.to_excel(writer, sheet_name='BidFeedback Results', index=False)

            # Set a password on the file
            password = secrets.token_urlsafe(DataSubjectAccessRequestTasks.PASSWORD_LENGTH)
            with open(temp_path, 'rb') as plain_file:
                encrypted = msoffcrypto.OfficeFile(plain_file)
                with open(final_path, 'wb') as encrypted_file:
                    encrypted.encrypt(password, encrypted_file)

            s3_utils.put_user_data(final_path, jira_ticket, file_name)
            s3_utils.put_user_data_backup(final_path, jira_ticket, file_name, dsr_item.request_is_us)
            url = cloudfront_signing_util.generate_signed_url(jira_ticket, file_name)

            ses_util.send_password_email(email, password, file_name)
            ses_util.send_download_email(email, url)

            jira_util.transition_jira_status([jira_ticket], jira_util.jira_done_status)
            jira_util.post_comment_to_ticket(jira_ticket, f'Final output password: {password}')

            os.unlink(temp_path)
            os.unlink(final_path)
        return True

    def create_finalize_output_task(self, airflow_dag):
        return OpTask(
            op=PythonOperator(
                task_id=DataSubjectAccessRequestTasks.FINALIZE_OUTPUT_TASK_ID,
                dag=airflow_dag,
                provide_context=True,
                python_callable=self._finalize_output,
                retry_exponential_backoff=True
            )
        )
