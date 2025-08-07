from ttd.ec2.cluster_params import ClusterParams


def get_spark_args(params: ClusterParams):
    spark_args = params.to_spark_arguments()
    args = [(key, str(value)) for key, value in spark_args['args'].items()]
    conf_args = [("conf", f"{key}={value}") for key, value in spark_args['conf_args'].items()]
    return conf_args + args
