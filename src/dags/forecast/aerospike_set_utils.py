import aerospike
from airflow.utils import timezone
from airflow.operators.python import PythonOperator

from ttd.tasks.op import OpTask

# YYYYMMddHHmm
date_time_minute_format_string = '%Y%m%d%H%M'
set_version_bin_name = 'sv'
timestamp_bin_name = 'ts'
ready_status_bin_name = 'rs'
lock_key_suffix = '_lock'
empty_gen_value = -1


###########################################
#   Get Inactive Set
###########################################
def get_inactive_aerospike_version(aerospike_client, namespace: str, metadata_set_name: str, set_key: str):
    print(f'Metadata Set Name: {metadata_set_name}')
    keyTuple = (namespace, metadata_set_name, set_key)
    (key, meta) = aerospike_client.exists(keyTuple)
    if meta is None:
        set_active_aerospike_version(aerospike_client, namespace, metadata_set_name, set_key, str(0))
        print("New entry has been created")

    (key, meta, bins) = aerospike_client.get(keyTuple)
    if set_version_bin_name not in bins:
        raise ValueError("setVersion bin not found")

    active_set_version = int(bins[set_version_bin_name])
    inactive_set_version = str(1 - active_set_version)
    print(f'Inactive set name: {set_key}_{inactive_set_version}')

    return inactive_set_version, meta['gen']


def get_inactive_set_version_and_lock_task(
    aerospike_addresses,
    namespace,
    metadata_set_name,
    set_key,
    inactive_xcom_set_number_key,
    aerospike_gen_xcom_key,
    inactive_xcom_set_key,
    **kwargs,
):

    (username, password) = get_aerospike_credentials()

    aerospike_client = create_aerospike_client(aerospike_addresses, username, password)

    try:
        (inactive_aerospike_set_number,
         aerospike_gen) = get_inactive_aerospike_version(aerospike_client, namespace, metadata_set_name, set_key)

        lock_aerospike_set(aerospike_client, namespace, metadata_set_name, set_key, str(inactive_aerospike_set_number))

        print(f'inactive_aerospike_set_number = {inactive_aerospike_set_number}')
        print(f'aerospike_gen = {aerospike_gen}')

        if inactive_xcom_set_key:
            inactive_aerospike_set = set_key + "_" + inactive_aerospike_set_number
            print(f'caching full set name aerospike_gen = {inactive_aerospike_set}')
            kwargs['task_instance'].xcom_push(key=inactive_xcom_set_key, value=inactive_aerospike_set)

        kwargs['task_instance'].xcom_push(key=inactive_xcom_set_number_key, value=inactive_aerospike_set_number)
        kwargs['task_instance'].xcom_push(key=aerospike_gen_xcom_key, value=aerospike_gen)

    finally:
        aerospike_client.close()


def create_get_inactive_set_version_and_lock_task(
    dag,
    task_id,
    aerospike_hosts,
    namespace,
    metadata_set_name,
    set_key,
    inactive_xcom_set_number_key,
    aerospike_gen_xcom_key,
    inactive_xcom_set_key=None
) -> OpTask:
    return OpTask(
        op=PythonOperator(
            dag=dag,
            task_id=task_id,
            python_callable=get_inactive_set_version_and_lock_task,
            op_args=[
                aerospike_hosts, namespace, metadata_set_name, set_key, inactive_xcom_set_number_key, aerospike_gen_xcom_key,
                inactive_xcom_set_key
            ],
            provide_context=True
        )
    )


###########################################
#   Activate Set
###########################################
def set_active_aerospike_version(
    aerospike_client, namespace: str, metadata_set_name: str, set_key: str, set_version: str, aerospike_gen: int = None
):
    cur_time = timezone.utcnow()

    cur_time_string = cur_time.strftime(date_time_minute_format_string)

    key = (namespace, metadata_set_name, set_key)
    bins = {set_version_bin_name: str(set_version), timestamp_bin_name: str(cur_time_string)}

    # these should have no ttl
    meta = {'ttl': -1}

    if aerospike_gen:
        meta['gen'] = aerospike_gen
        # Ensures that if we update, we are updating the exact value we had in mind.
        aerospike_client.put(
            key, bins, meta=meta, policy={
                'gen': aerospike.POLICY_GEN_EQ,
                'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
            }
        )
    else:
        aerospike_client.put(key, bins, meta=meta)


def activate_and_unlock_set_version_task(
    aerospike_addresses, task_id, job_name, namespace, metadata_set_name, set_key, inactive_xcom_set_number_key, aerospike_gen_xcom_key,
    **kwargs
):
    inactive_aerospike_set_number = str(
        kwargs['task_instance'].xcom_pull(dag_id=job_name, task_ids=task_id, key=inactive_xcom_set_number_key)
    )
    aerospike_gen = int(kwargs['task_instance'].xcom_pull(dag_id=job_name, task_ids=task_id, key=aerospike_gen_xcom_key))
    inactive_aerospike_set = set_key + "_" + inactive_aerospike_set_number
    print(f'activating set number {inactive_aerospike_set}')

    (username, password) = get_aerospike_credentials()

    aerospike_client = create_aerospike_client(aerospike_addresses, username, password)

    try:
        set_active_aerospike_version(aerospike_client, namespace, metadata_set_name, set_key, inactive_aerospike_set_number, aerospike_gen)
        unlock_aerospike_set(aerospike_client, namespace, metadata_set_name, set_key, inactive_aerospike_set_number)
    finally:
        aerospike_client.close()


def create_activate_and_unlock_set_version_task(
    dag, task_id, aerospike_hosts, inactive_get_task_id, job_name, namespace, metadata_set_name, set_key, inactive_xcom_set_number_key,
    aerospike_gen_xcom_key
) -> OpTask:
    return OpTask(
        op=PythonOperator(
            dag=dag,
            task_id=task_id,
            python_callable=activate_and_unlock_set_version_task,
            op_args=[
                aerospike_hosts, inactive_get_task_id, job_name, namespace, metadata_set_name, set_key, inactive_xcom_set_number_key,
                aerospike_gen_xcom_key
            ],
            provide_context=True
        )
    )


###########################################
#   Get Lock Status For Set
###########################################
def check_lock_status_aerospike_set(aerospike_client, namespace: str, metadata_set_name: str, set_key: str, set_version: str):
    expected_locked_set_version = str(set_version)

    lock_set_key = set_key + lock_key_suffix

    aerospike_key = (namespace, metadata_set_name, lock_set_key)

    (key, meta) = aerospike_client.exists(aerospike_key)
    is_locked = False
    # default to neg value. If there's no existing lock we won't get one.
    aerospike_gen = empty_gen_value
    if meta is not None:
        (key, meta, bins) = aerospike_client.get(aerospike_key)
        aerospike_gen = meta['gen']
        if set_version_bin_name not in bins:
            raise ValueError("setVersion bin not found")
        locked_set_version = bins[set_version_bin_name]
        if expected_locked_set_version == locked_set_version:
            # found lock on set and version
            is_locked = True

    # No explicit lock on set and version.
    return is_locked, aerospike_gen


###########################################
#   Lock Set For Import
###########################################
def lock_aerospike_set(aerospike_client, namespace: str, metadata_set_name: str, set_key: str, set_version: str):
    cur_time = timezone.utcnow()

    cur_time_string = cur_time.strftime(date_time_minute_format_string)

    lock_set_key = set_key + lock_key_suffix  # rv3_lock

    key = (namespace, metadata_set_name, lock_set_key)
    bins = {set_version_bin_name: str(set_version), timestamp_bin_name: str(cur_time_string)}

    # We need to make sure that there will always be a set available that isn't locked for import.
    # Otherwise, we will get into a state where ALL sets are not safe for use for ram service.
    other_set_version = 1 - int(set_version)
    (is_other_set_locked,
     aerospike_gen) = check_lock_status_aerospike_set(aerospike_client, namespace, metadata_set_name, set_key, str(other_set_version))
    if is_other_set_locked:
        raise ValueError(set_key + "_" + str(other_set_version) + " must be unlocked before locking " + set_key + "_" + set_version)

    # these should have no ttl
    meta = {'ttl': -1}
    if aerospike_gen != empty_gen_value:
        meta['gen'] = aerospike_gen
        aerospike_client.put(
            key, bins, meta=meta, policy={
                'gen': aerospike.POLICY_GEN_EQ,
                'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
            }
        )
    else:
        aerospike_client.put(key, bins, meta=meta)


###########################################
#   Unlock Set For Import
###########################################
def unlock_aerospike_set(aerospike_client, namespace: str, metadata_set_name: str, set_key: str, set_version: str):

    expected_locked_set_version = str(set_version)

    lock_set_key = set_key + lock_key_suffix

    key = (namespace, metadata_set_name, lock_set_key)

    (key, metadata, bins) = aerospike_client.get(key)

    if set_version_bin_name not in bins:
        raise ValueError("setVersion bin not found")
    locked_set_version = bins[set_version_bin_name]

    if expected_locked_set_version == locked_set_version:
        aerospike_gen = metadata['gen']

        meta = {'gen': aerospike_gen}
        aerospike_client.remove(key, meta=meta, policy={'gen': aerospike.POLICY_GEN_EQ, 'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL})

        print(f'finished removing with aerospike gen = {aerospike_gen}')
    else:
        raise ValueError("Expected to unlock " + expected_locked_set_version + " but found " + locked_set_version)


def create_aerospike_client(aerospike_addresses: str, username: str, password: str):

    # We split the address into a configuration object of this format: {'hosts' : [ ('10.100.149.150', 3000), ('10.100.155.198', 3000) ]}
    aerospike_config = {
        'hosts': [(h, int(p)) for (h, p) in [tuple(a.strip().split(':')) for a in aerospike_addresses.split(',')]],
        'user': username,
        'password': password
    }
    aerospike_client = aerospike.client(aerospike_config).connect()

    return aerospike_client


###########################################
#   Get Aerospike Credentials
###########################################
def get_aerospike_credentials():
    from airflow.models import Variable
    # TODO: FORECAST-4730 move away from master credentials
    return Variable.get('aerospike-username'), Variable.get('aerospike-password')
