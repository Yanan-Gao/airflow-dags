#!/bin/sh

# Set the local variable based on the user name environment variable value
if [ -n "$MACOS_USERNAME" ]; then
    username=$MACOS_USERNAME
else
    username=$WINDOWS_USERNAME
fi

echo "$username"

push_startup_metrics() {
  echo "Executing startup push metrics..."
  echo "airflow_local_status 1" | curl --data-binary @- http://prom-push-gateway.adsrvr.org:80/metrics/job/ttd_airflow2_local/instance/$username
  echo "airflow_local_start $(date +%s)" | curl --data-binary @- http://prom-push-gateway.adsrvr.org:80/metrics/job/ttd_airflow2_local/instance/$username
  echo "Finished pushing startup metrics... metric: airflow_local_status 1"
  echo "metric: airflow_local_start $(date +%s)"
}

# Function to handle the SIGTERM signal
push_shutdown_metrics() {
  # Add your shutdown logic here
  echo "Executing shutdown push metrics..."
  echo "airflow_local_status 0" | curl --data-binary @- http://prom-push-gateway.adsrvr.org:80/metrics/job/ttd_airflow2_local/instance/$username
  echo "airflow_local_stop $(date +%s)" | curl --data-binary @- http://prom-push-gateway.adsrvr.org:80/metrics/job/ttd_airflow2_local/instance/$username
  echo "Finished pushing shutdown metrics... metric: airflow_local_status 0"
  echo "metric: airflow_local_stop $(date +%s)"
}

push_startup_metrics

# Trap the SIGTERM signal and call the push_shutdown_metrics function
trap push_shutdown_metrics TERM

# Keep the script running to prevent container from exiting
while true; do sleep 1; done
