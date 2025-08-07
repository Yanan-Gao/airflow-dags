## Using the connection formatter tool

### Prerequisites 

All connection objects in Airflow have the same structure:
```python
    conn_id: str 
    conn_type: str 
    host: str 
    login: str 
    password: str 
    schema: str 
    port: int 
    extra: str
```

Here are a couple of examples of various connection types fields arrangements:

* [PostgreSQL](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html)
* [Hashicorp Vault](https://airflow.apache.org/docs/apache-airflow-providers-hashicorp/stable/connections/vault.html)
* [Snowflake](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html)

### Running the tool

#### To get a formatted connection URI, you can run one of the following commands:

This approach doesn't require any apps/modules installed except Docker:
```shell
docker run --rm \
    --volume ./local_tools/connection_formatter:/app \
    --entrypoint "python3" \
    dev.docker.adsrvr.org/ttd-airflow-test:2.8.1-1.16 \
    /app/connection_formatter.py \
    --conn-type "<connection-type>" --login "<login>" --password "<password>" \
    --host "<host>" --port <port-number> --schema "<schema>" --extra "<extra-json>"
```
Here the last two lines are parameters of the connection: it's not a requirement to specify all of them, but at least one argument should be present.

If you happened to have Python airflow module installed on your machine, there is also an option to run the tool as:
```shell
python3 ./local_tools/connection_formatter/connection_formatter.py \
    --conn-type "<connection-type>" --login "<login>" --password "<password>" \
    --host "<host>" --port <port-number> --schema "<schema>" --extra "<extra-json>"
```

### Putting formatted connection to the TTD Vault

Log in using the vault CLI
```shell
vault login -address=https://vault.adsrvr.org -method=oidc  -path=ops-sso
```

Put the formatted connection string as a secret:
```shell
vault kv put --address=https://vault.adsrvr.org \
    --namespace=team-secrets \
    --mount=secret SCRUM-<team>/Airflow/Connections/<connection-name> conn_uri=<tool-output>
```


