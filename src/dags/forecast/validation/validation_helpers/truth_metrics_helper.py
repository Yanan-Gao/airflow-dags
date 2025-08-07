import logging

from dags.forecast.validation.validation_helpers.aws_helper import download_json_from_s3, download_single_json_from_s3


def get_high_confidence_adgroups_batched_strings(run_date, batch_size=5000):
    try:
        import pandas as pd

        hc_adgroups = download_single_json_from_s3(
            bucket_name='ttd-forecasting-useast',
            file_key=f'env=prod/forecast-validation/raw-stable-adgroups-spend-metrics/date={run_date}/cumulative-spend-metrics.json'
        )
        hc_adgroups = hc_adgroups[["AdGroupId", "StartDate", "StablenessLength"]]
        logging.info(f"Fetched {len(hc_adgroups)} High Confidence Adgroups from S3.")

        logging.info("Casting StartDate columns to datetime.")
        hc_adgroups['StartDate'] = pd.to_datetime(hc_adgroups['StartDate'])

        logging.info("Converting StablenessLength to int.")
        hc_adgroups['StablenessLength'] = hc_adgroups['StablenessLength'].astype(int)

        hc_adgroups['EndDate'] = hc_adgroups['StartDate'] + pd.to_timedelta(hc_adgroups['StablenessLength'], unit='D')

        hc_adgroups = hc_adgroups[['AdGroupId', 'StartDate', 'EndDate']].drop_duplicates()

        logging.info(f"Splitting into batches of {batch_size} rows.")
        batched_strings = []
        for start in range(0, len(hc_adgroups), batch_size):
            batch = hc_adgroups.iloc[start:start + batch_size]
            batch_str = ",".join(f"('{row.AdGroupId}', '{row.StartDate}', '{row.EndDate}')" for row in batch.itertuples())
            batched_strings.append(batch_str)

        logging.info(f"Generated {len(batched_strings)} batch(es).")

        return batched_strings

    except Exception as e:
        logging.error(f"Failed getting High Confidence Adgroups data batched strings with error: {e}")
        raise


def get_stable_adgroups_batched_strings(run_date, minimum_created_at_str: str, threshold=10000, batch_size=5000):
    try:
        import pandas as pd

        raw_stable_adgroups = download_json_from_s3(
            bucket_name='ttd-forecasting-useast', prefix=f'env=prod/forecast-validation/raw-stable-lag-set/stable-adgroups-{run_date}/'
        )
        logging.info(f"Fetched {len(raw_stable_adgroups)} stable adgroups from S3.")

        logging.info("Casting start_date and CreatedAt columns to datetime.")
        raw_stable_adgroups['CreatedAt'] = pd.to_datetime(raw_stable_adgroups['CreatedAt'], format='mixed')
        raw_stable_adgroups['start_date'] = pd.to_datetime(raw_stable_adgroups['start_date'], format='mixed')

        logging.info(f"Filtering out stable adgroups longer than 90 days and created before {minimum_created_at_str}")
        filtered_df = raw_stable_adgroups.loc[
            (raw_stable_adgroups["stableness_length"] < 90) & (raw_stable_adgroups["CreatedAt"] > pd.Timestamp(minimum_created_at_str)),
            ["AdGroupId", "PartnerId", "AdvertiserId", "CampaignId", "start_date", "CreatedAt", "stableness_length"]]

        logging.info(f"RawAdGroups count after filtering: {len(filtered_df)}")

        logging.info("Truncating datetime at hour level.")
        filtered_df['start_date_rounded_down'] = filtered_df['start_date'].dt.floor('H')
        filtered_df['createdAt_date_rounded_down'] = filtered_df['CreatedAt'].dt.floor('H')

        logging.info("Converting stableness to int.")
        filtered_df['stableness_length_rounded_down'] = filtered_df['stableness_length'].astype(int)

        logging.info("Selecting only the needed columns.")
        final_filtered_df = filtered_df[[
            "AdGroupId", "PartnerId", "AdvertiserId", "CampaignId", "start_date_rounded_down", "createdAt_date_rounded_down",
            "stableness_length_rounded_down"
        ]].drop_duplicates().head(threshold)

        logging.info(f"Splitting into batches of {batch_size} rows.")
        batched_strings = []
        for start in range(0, len(final_filtered_df), batch_size):
            batch = final_filtered_df.iloc[start:start + batch_size]
            batch_str = ",".join(
                f"('{row.AdGroupId}', '{row.PartnerId}', '{row.AdvertiserId}', "
                f"'{row.CampaignId}', '{row.start_date_rounded_down}', '{row.createdAt_date_rounded_down}', '{row.stableness_length_rounded_down}')"
                for row in batch.itertuples()
            )
            batched_strings.append(batch_str)

        logging.info(f"Generated {len(batched_strings)} batch(es).")

        return batched_strings

    except Exception as e:
        logging.error(f"Failed getting stable adgroups data batched strings with error: {e}")
        raise


def get_daily_aggregated_truth_metrics(df):
    try:
        logging.info("Aggregating metrics into lists.")
        aggregated_df = df.groupby('AdGroupId').agg({
            'day_number': 'max',
            'start_date': 'min',
            'BidCount': lambda x: x.tolist(),
            'ImpressionCount': lambda x: x.tolist(),
            'BidAmountInUSD': lambda x: x.tolist(),
            'AdvertiserCostInUSD': lambda x: x.tolist(),
            'IdReach': lambda x: x.tolist(),
            'PersonReach': lambda x: x.tolist(),
            'HouseholdReach': lambda x: x.tolist()
        }).reset_index()

        aggregated_df.rename(
            columns={
                'day_number': 'StablenessLength',
                'start_date': 'StartDate',
                'daily_spend': 'DailyAdvertiserCostInUSD'
            }, inplace=True
        )

        logging.info(f"Got {len(aggregated_df)} high confidence adgroups.")

        return aggregated_df
    except Exception as e:
        logging.error(f"Failed getting daily aggregated truth metrics with exception: {e}")
        raise


def get_cumulative_spend_truth(daily_metrics):
    try:
        logging.info("Adding cumulative spend column.")
        daily_metrics['cm_bid_count'] = daily_metrics.groupby('AdGroupId')['BidCount'].cumsum()
        daily_metrics['cm_impression_count'] = daily_metrics.groupby('AdGroupId')['ImpressionCount'].cumsum()
        daily_metrics['cm_bid_amount_in_usd'] = daily_metrics.groupby('AdGroupId')['BidAmountInUSD'].cumsum()
        daily_metrics['cm_spend'] = daily_metrics.groupby('AdGroupId')['AdvertiserCostInUSD'].cumsum()

        logging.info("Restructuring dataframe.")
        df_cumulative_spend = daily_metrics.groupby('AdGroupId').agg({
            'day_number': 'max',
            'start_date': 'min',
            'cm_bid_count': lambda x: x.tolist(),
            'cm_impression_count': lambda x: x.tolist(),
            'cm_bid_amount_in_usd': lambda x: x.tolist(),
            'cm_spend': lambda x: x.tolist()
        }).reset_index().rename(
            columns={
                'cm_bid_count': 'CumulativeBidCount',
                'cm_impression_count': 'CumulativeImpressionCount',
                'cm_bid_amount_in_usd': 'CumulativeBidAmountInUSD',
                'cm_spend': 'CumulativeAdvertiserCostInUSD',
                'start_date': 'StartDate',
                'day_number': 'StablenessLength'
            }
        )

        return df_cumulative_spend
    except Exception as e:
        logging.error(f"Failed getting cumulative spend truth data with exception: {e}")
        raise
