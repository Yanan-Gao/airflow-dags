"""
Helper code for handling DSDR requests
"""
import logging
from collections import namedtuple

import csv
from datetime import timedelta
from typing import Dict

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage


class DsdrRequest(namedtuple('DsdrRequest', 'requestId,timestamp,advertiserId,partnerId,tenantId,userIdGuid,rawUserId,userIdType')):

    def __new__(cls, *args):
        return super(DsdrRequest, cls).__new__(cls, *args)


def get_agg_prefix(dt, prod_dev_folder_prefix):
    monday = dt - timedelta(days=dt.weekday())
    last_monday = monday - timedelta(days=7)
    return f'{prod_dev_folder_prefix}/partnerdsr/enriched/{last_monday.strftime("%Y-%m-%d")}to{monday.strftime("%Y-%m-%d")}/'


def fetch(s3_hook: AwsCloudStorage, bucket: str, prefix: str):
    logging.info(f'Fetching dsdr requests from cloud, bucket: {bucket}, prefix: {prefix}')
    dsr_keys = s3_hook.list_keys(prefix=prefix, bucket_name=bucket)

    dsr_file_keys = (key for key in dsr_keys if not key.endswith('/'))
    if not dsr_file_keys:
        return {}

    dsdr_requests = []
    dsdr_request_ids = set()
    for key in dsr_file_keys:
        obj = s3_hook.read_key(key, bucket_name=bucket)
        lines = obj.splitlines()
        records = csv.reader(lines)
        headers = next(records)
        print('headers: %s' % headers)
        for record in records:
            dsdr_requests.append(DsdrRequest(*record))
            dsdr_request_ids.add(record[0])

    id_graph: Dict[str, Dict[str, Dict[str, list[str]]]] = dict()
    for request in dsdr_requests:
        partner_id: str = request.partnerId.lower()
        advertiser_id: str = request.advertiserId.lower()
        userid_type: str = request.userIdType.lower()
        userid_guid: str = request.userIdGuid.lower()
        raw_userid: str = request.rawUserId.lower()
        print(f'Request has guid: {userid_guid} with rawUserId: {raw_userid}')

        advertiser_graph = id_graph.get(partner_id, dict[str, dict[str, list[str]]]())
        user_ids_graph = advertiser_graph.get(advertiser_id, dict[str, list[str]]())
        user_ids: list[str] = user_ids_graph.get(userid_type, list[str]())

        if userid_guid and userid_guid not in user_ids:
            user_ids.append(userid_guid)
        if raw_userid and raw_userid not in user_ids:
            user_ids.append(raw_userid)

        if user_ids:
            user_ids_graph[userid_type] = user_ids
            advertiser_graph[advertiser_id] = user_ids_graph
            id_graph[request.partnerId] = advertiser_graph
        else:
            print(f'No id found for request {request.requestId}')

    return id_graph, dsdr_request_ids
