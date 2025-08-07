"""
This canary DAG verifies that the SSL certificates for the given URLs are available to python.
"""

import sys
from datetime import datetime, timedelta

from ttd.eldorado.base import TtdDag

from ttd.tasks.op import OpTask
from ttd.tasks.python import PythonOperator
from ttd.ttdenv import TtdEnvFactory
import logging
import ssl
import socket

schedule_interval = None
max_active_runs = 5

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = timedelta(hours=1)
    max_active_runs = 1

URLS_TO_CHECK = [("eng-teams.gen.adsrvr.org", 443), ("lineage-proxy.gen.adsrvr.org", 443), ("lineage-proxy.dev.gen.adsrvr.org", 443),
                 ("vault.adsrvr.org", 443), ("agiles-east.clickhouse.prod.use1.adsrvr.org", 9440), ("www.slack.com", 443)]

dag = TtdDag(
    dag_id="ssl-canary-dag",
    schedule_interval=schedule_interval,
    max_active_runs=max_active_runs,
    tags=["Canary", "Regression", "Monitoring", "Operations"],
    run_only_latest=True,
    start_date=datetime(2024, 12, 6),
    depends_on_past=False,
    retries=2,
    retry_delay=timedelta(minutes=5),
)


def verify_ssl_socket(hostname: str, port: int):
    context = ssl.create_default_context()
    try:
        logging.info(f"Verifying SSL for {hostname}")
        with socket.create_connection((hostname, port)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert()
                logging.info("✅ SSL certificate is valid.")
                if cert is not None:
                    logging.info(f"Cert subject: {cert['subject']}, issuer: {cert['issuer']}")
    except ssl.SSLCertVerificationError as e:
        logging.info("❌ SSL verification failed for url %s:", hostname, e)
        sys.exit(1)
    except Exception as e:
        logging.info("❌ Connection failed, unable to verify SSL:", e)


def verify_urls():
    for url, port in URLS_TO_CHECK:
        verify_ssl_socket(url, port)


verify_ssl = OpTask(op=PythonOperator(
    task_id='verify_urls',
    python_callable=verify_urls,
    provide_context=True,
))

adag = dag.airflow_dag

dag >> verify_ssl
