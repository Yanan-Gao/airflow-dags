The proto is defined in [contract package](https://gitlab.adsrvr.org/thetradedesk/teams/forecast/ttd.forecasting.historyservice/-/tree/master/src/contract/Protos)

If you change it, you need to update the files in core.

The steps to follow are described in https://grpc.io/docs/languages/python/basics/

1. Install the tool (Recommend using a virtual environment)
`pip install grpcio-tools==1.70.0`
2. Update the generated protocol buffer code. It takes 4 parameters: 
   1. `-I folder` folder containing the proto file (used for imports)
   2. `--python_out folder` destination for the _pb2.py file
   3. `--grpc_python_out` destination for the pb2_grpc.py file
   4. `file` proto file
   5. More info on parameters is available running `python -m grpc_tools.protoc --help`
```shell
python -m grpc_tools.protoc -I=/Users/$USERNAME/ttdsrc/ttd.forecasting.historyservice/src/contract/Protos --python_out=. --grpc_python_out=. /Users/$USERNAME/ttdsrc/ttd.forecasting.historyservice/src/contract/Protos/forecasting_hist.proto
```
3. Update the generated file forecasting_hist_pb2_grpc.py import to reflect the actual folder structure - otherwise the
DAG will fail the GitLab pipeline
```diff
-import forecasting_hist_pb2 as forecasting_hist_pb2
+import dags.forecast.history_service.core.forecasting_hist_pb2 as forecasting__hist__pb2
```