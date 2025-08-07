from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset

adgroup_exclusion_threshold: DateGeneratedDataset = DateGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    env_aware=True,
    data_name="models/audienceautogeneration/audience_excluder/adgroup_exclusion_threshold",
    version=1,
    date_format="date=%Y%m%d",
)

sib_rolling_device_data_uniques: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    env_aware=True,
    data_name="seeninbiddingrollingdevicedatauniques/v=2",
    version=None,
    date_format="date=%Y%m%d",
    hour_format="hour={hour:0>2d}",
    success_file="_SIB_DATE"
)

targeting_data_permissions_rolling: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    env_aware=True,
    data_name="models/lal/targeting_data_permissions_rolling/v=2/isxd=false",
    version=None,
    date_format="date=%Y%m%d",
    hour_format="hour={hour:0>2d}",
    success_file="_SUCCESS"
)

bidrequest_agg: DateGeneratedDataset = DateGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    env_aware=True,
    data_name="models/audienceautogeneration/audience_excluder/bidrequest_agg",
    version=1,
    date_format="date=%Y%m%d",
    success_file="_COUNT"
)

model_results_for_aerospike_rolling: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    env_aware=True,
    data_name="models/lal/model_results_for_aerospike_rolling/v=1",
    version=None,
    date_format="date=%Y%m%d",
    hour_format="hour={hour:0>2d}",
    success_file="_SUCCESS"
)

all_model_results_rolling: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    env_aware=True,
    data_name="models/lal/all_model_results_rolling/v=2/isxd=false",
    version=None,
    date_format="date=%Y%m%d",
    hour_format="hour={hour:0>2d}",
    success_file="_SUCCESS"
)


def get_rsm_id_relevance_score(split, env="prod"):
    return DateGeneratedDataset(
        bucket="thetradedesk-mlplatform-us-east-1",
        path_prefix=f"data/{env}",
        env_aware=False,
        data_name="audience/scores/tdid2seedid",
        version=1,
        date_format=f"date=%Y%m%d/split={split}",
        success_file=None
    )


def get_rsm_seed_ids(env="prod"):
    return DateGeneratedDataset(
        bucket="thetradedesk-mlplatform-us-east-1",
        path_prefix=f"data/{env}",
        env_aware=False,
        data_name="audience/scores/seedids",
        version=2,
        date_format="date=%Y%m%d",
        success_file="_SUCCESS"
    )


def get_rsm_seed_population_scores(env="prod"):
    return DateGeneratedDataset(
        bucket="thetradedesk-mlplatform-us-east-1",
        path_prefix=f"data/{env}",
        env_aware=False,
        data_name="audience/scores/seedpopulationscore",
        version=1,
        date_format="date=%Y%m%d",
        success_file="_SUCCESS"
    )
