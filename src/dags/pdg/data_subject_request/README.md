# Data Subject Request Processing

A Data Subject Request (DSR) is a submission by an individual (data subject) to a business asking to access or delete what personal information of theirs has been stored and/or how that data is being used.

## Overview
This subdirectory is meant to store Airflow DAGs related to processing data subject requests.

See the initial design [here](https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?pageId=177903925)

We will be utilizing PythonOperators to make http requests to the relevant services requiring processing.  We will utilize AWS EMR to spin up spark jobs that will handle all long-running processes.  The associated Scala code that we are running in spark will live [in this repo](https://gitlab.adsrvr.org/thetradedesk/teams/pdg/data-subject-request-processing/-/tree/master).

[Infrastructure code](https://gitlab.adsrvr.org/thetradedesk/teams/cloud/infradesc/-/blob/master/teams/privacy-and-data-governance/data-subject-request.jsonnet)

## Delete Request
The `data-governance-dsr-delete-automation` DAG is responsible for deleted the data of provided users as well as ensuring they do not receive specific-targeted ads from TTD. 

Vertica deletion will be handled by a combination of cleansing (via the [VerticaScrub](https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/LogProcessing/LogExtractor/TTD.Domain.LogProcessing.LogExtractor/Extractions/VerticaScrub/VerticaScrubExtraction.cs)) 
for tables that have retention periods longer than the allowable 45 days to processes DSR deletes and purging tables that have 
[retention periods](https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/DB/Provisioning/ProvisioningDB/Scripts/Post-Deployment/PopulateVerticaDataRetentionPolicy.sql) 
under 45 days (via [VerticaDataPurgeTask](https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/TaskExecution/TaskService/TTD.Domain.TaskExecution.TaskService/Tasks/Vertica/DataPurge/VerticaDataPurgeTask.cs#L12)).

Vertica Scrub generation is now handled via Airflow as seen [here](https://gitlab.adsrvr.org/thetradedesk/airflow-dags/blob/85ec44a148196d494ca7e4c722b1e484bcd4f7ea/dags/jobs/spark_jobs/data_governance/data_subject_request/handler/vertica_scrub_generator_creator.py)

## Access Request

The `data-governance-dsr-access-automation` DAG is responsible for gathering user data for each of the provided users from 
our platform and produces an output which can be returned to users requesting such information.

## Local Testing
Follow the instructions in the top-level README to spin up a local docker instance of airflow. Once the docker image
has been deployed, code changes can be made without running docker compose again. After running docker compose, it
could take several minutes before the DAGs are available in the UI.

### AWS

AWS credentials need to be present in environment variables before running docker compose.
The production-account credentials are required for things like uploading files to S3 when testing.
In addition, make sure the region is set (also before running docker compose):

```
export AWS_DEFAULT_REGION="us-east-1"
```

### Variables

The DSR automation uses several variables for access to different systems. The code uses airflow's `Variable.get('')`
functionality to obtain values for these variables.  In the production and prodtest environments, the values for these
variables come from Vault.  For PDG, the values can be found [here](https://vault.adsrvr.org/ui/vault/secrets/secret/kv/list/SCRUM-PDG/Airflow/Variables/?namespace=team-secrets).

NOTE: These are production values and provide production access to several systems. Use caution when
running code with production values.

When testing locally, you can load the values into your local airflow after spinning up the docker image. The variables can be found at [this local URL](http://localhost:3000/variable/list/).
In order to populate values for these variables, you can load them into the following JSON using the values obtained from Vault.

```
{
    "aerospike-endpoint-name": "",
    "aerospike-password": "",
    "aerospike-user-id": "",
    "ttd-readonly-password": "",
    "vertica-password": "",
    "vertica-user": ""
}
```

Save the file and then use the "Import Variables" functionality on the airflow variable page.