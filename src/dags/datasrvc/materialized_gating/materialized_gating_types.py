MATERIALIZED_GATING_TYPES = {
    # single cluster checker dependency task and gating types
    'VerticaMergeIntoDataElementReport': '90015',
    'VerticaMergeIntoFeeFeaturesReport': '90016',
    'VerticaMergeIntoPerformanceReport': '90017',
    'VerticaMergeIntoRTBPlatformReport': '90018',
    'VerticaMergeIntoCumulativePerformanceReport': '90019',
    'VerticaMergeIntoCategoryElementReport': '90032',

    # cross cluster checker dependency task and gating types
    'ImportPerformanceReportFromAzure': '90020',
    'ImportPlatformReportFromAzure': '90021',
    'ImportPlatformDataElementReportFromAzure': '90022',
    'ImportPlatformFeeFeatureReportFromAzure': '90023',
    'ImportPerformanceReportFromChina': '90024',
    'ImportPlatformReportFromChina': '90025',
    'ImportPlatformDataElementReportFromChina': '90026',
    'ImportPlatformFeeFeatureReportFromChina': '90027',
    'ImportPlatformCategoryElementReportFromAzure': '90039',
    'ImportPlatformCategoryElementReportFromChina': '90040',

    # raw data cloud tenant checker dependency task and gating types
    'VerticaLoadBidRequest': '90000',
    'VerticaLoadBidFeedback': '90028',
    'VerticaLoadClickTracker': '90029',
    'VerticaLoadConversionTracker': '90030',
    'VerticaLoadVideoEvent': '90031',

    # late data checker dependency task and gating types
    'VerticaLateDataMergeIntoPerformanceReport': '90008',
    'VerticaLateDataMergeIntoPlatformReport': '90009',
    'IntegralVerticaMergeIntoPerformanceReport': '90033',
    'IntegralVerticaMergeIntoPlatformReport': '90034',
    'ImportIntegralPerformanceReportFromAzure': '90035',
    'ImportIntegralPlatformReportFromAzure': '90036',
    'VerticaViewabilityMergeIntoPlatformReport': '90037',
    'VerticaViewabilityCopyIntoPerformanceReport': '90038'
}
