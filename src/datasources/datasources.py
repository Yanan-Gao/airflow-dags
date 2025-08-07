from datasources.sources.avails_datasources import AvailsDatasources
from datasources.sources.contentlibrary_datasources import ContentLibraryDatasources
from datasources.sources.ctv_datasources import CtvDatasources
from datasources.sources.datprd_datasources import DatPrdDatasources
from datasources.sources.dooh_datasources import DoohDatasources
from datasources.sources.fm_datasources import ForwardMarketDataSources
from datasources.sources.lal_datasources import LalDatasources
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from datasources.sources.samba_datasources import SambaDatasources
from datasources.sources.common_datasources import CommonDatasources
from datasources.sources.segment_datasources import SegmentDatasources
from datasources.sources.sib_datasources import SibDatasources
from datasources.sources.test_datasources import TestDatasources
from datasources.sources.sql_synced_datasources import SQLSyncedDataSources
from datasources.sources.perfauto_datasources import PerformanceAutomationDatasources
from datasources.sources.coldstorage_lookup_datasources import (
    ColdstorageLookupDataSources,
)
from datasources.sources.tivo_datasources import TivoDatasources
from datasources.sources.fwm_datasources import FwmDatasources
from datasources.sources.experian_datasources import ExperianDatasources
from datasources.sources.xdgraph_datasources import XdGraphDatasources
from datasources.sources.contextual_datasources import ContextualDatasources


class Datasources:
    ################################################################
    # ACR Data Sources
    ################################################################
    samba = SambaDatasources
    tivo = TivoDatasources
    fwm = FwmDatasources

    experian = ExperianDatasources

    segment = SegmentDatasources

    ################################################################
    # Forecasting Data Sources
    ################################################################
    coldstorage = ColdstorageLookupDataSources

    ################################################################
    # Data synced from SQL Server
    ################################################################
    sql = SQLSyncedDataSources

    ################################################################
    # Everything Else
    ################################################################
    ctv = CtvDatasources
    datprd = DatPrdDatasources
    common = CommonDatasources
    forwardmarket = ForwardMarketDataSources
    dooh = DoohDatasources
    avails = AvailsDatasources
    sib = SibDatasources
    rtb_datalake = RtbDatalakeDatasource
    xdgraph = XdGraphDatasources
    lal = LalDatasources
    contextual = ContextualDatasources
    perfauto = PerformanceAutomationDatasources
    contentlibrary = ContentLibraryDatasources

    ################################################################
    # Tests
    ################################################################
    test = TestDatasources

    ################################################################
    # Static Data Sources
    #
    # IMPORTANT
    # This is for STATIC files and directories only, like a
    # static/adhoc .json file.
    #
    # If your file is updated regularly or has monthly, daily,
    # hourly, etc partitions/directories then you want a Dataset!
    ################################################################
