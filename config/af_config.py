

HOME_APP_URL_PID = "https://hq1.appsflyer.com/app/apps/get-adnet-apps"

HOME_APP_URL_PRT = "https://hq1.appsflyer.com/cdp/get-data?fullAccountData=true"

TABLE_URL = "https://hq1.appsflyer.com/unified/data?widget=ltv-table:3"
TABLE_EXPAND_URL = "https://hq1.appsflyer.com/unified/data?widget=ltv_table_expand"
TABLE_EXPAND_INTERVAL = 3
TABLE_DATA_COUNT_LIMIT = 50

APP_INFO = "https://hq1.appsflyer.com/cdp/get-data?appId="
USE_CACHE_COOKIE = True

LOGIN_API = "https://hq1.appsflyer.com/auth/login"

GROUP_FILTER_PRT = {
        "filters":{"app_id":["need_set"],},
        "kpis":["installs","installs_reattr","installs_retarget","installs_ua","impressions","clicks","revenue","sessions","roi","arpu","avg_ecpi","install_cost","click_installs","impression_installs","conv_rate","loyals","loyal_rate","uninstalls","uninstall_rate","roas","gross_profit","ctr","cpm","cpc"],
        "get_complementary_data":True,
        "is_primary_inapps":True,
        "start_date":"need_set",
        "sort_by":[["combined_conversions","desc"]],
        "limit":"need_set",
        "end_date":"need_set",
        "exclusions":{},
        "groupings":"need_set",
        "event_kpis":["conversion_rate","unique_users","count","revenue","ecpa","install_rate"],
        "is_apps_timezone":True,
        "is_apps_currency":True
    }

GROUP_FILTER_PID= GROUP_FILTER_PRT.copy()
GROUP_FILTER_PID["filters"]["event_name"] = ["app_initial_open","signup","ftd"]

NEW_TABLE_API = "https://hq1.appsflyer.com/platform/dashboard?widget=platform-table:0"
NEW_TABLE_API_REFERER = "https://hq1.appsflyer.com/unified-ltv/dashboard"
NEW_TABLE_API_PARAM = {
    "dates":{"start":"2025-06-01","end":"2025-06-30"},
    "filters":{"app-id":["com.bybit.app"]},
    "view-type":"unified",
    "localization":{"timezone":"UTC","currency":"USD"},
    "groupings":[{"dimension":"adset","limit":100}],
    "summations":["totals","others"],
    "metrics":[{"metric-id":"impressions","filters":{},"granularity":"","category":"core","period":"","platform-id":"filtersGranularityMetricIdImpressionsPeriod"},{"metric-id":"clicks","filters":{},"granularity":"","category":"core","period":"","platform-id":"filtersGranularityMetricIdClicksPeriod"},{"metric-id":"installs","attribution-source":"appsflyer","filters":{},"granularity":"","sort-by":{"order":"desc","priority":0},"category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsPeriod"},{"metric-id":"installs-ua","filters":{},"attribution-source":"appsflyer","granularity":"","category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsUaPeriod"},{"metric-id":"installs-reattr","filters":{},"attribution-source":"appsflyer","granularity":"","category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsReattrPeriod"},{"metric-id":"installs-retarget","filters":{},"attribution-source":"appsflyer","granularity":"","category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsRetargetPeriod"},{"metric-id":"installs-cost","filters":{},"granularity":"","category":"core","period":"","platform-id":"filtersGranularityMetricIdInstallsCostPeriod"},{"metric-id":"ecpi","filters":{},"attribution-source":"appsflyer","granularity":"","category":"calculated","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdEcpiPeriod"},{"metric-id":"revenue","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"activity","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRevenuePeriodActivity"},{"metric-id":"revenue","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRevenuePeriodLtv"},{"metric-id":"roas","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"1","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRoasPeriod1"},{"metric-id":"roas","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"7","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRoasPeriod7"},{"metric-id":"roas","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRoasPeriodLtv"},{"metric-id":"retention-rate","filters":{},"attribution-source":"appsflyer","aggregation-type":"on-period","granularity":"days","category":"calculated","period":"1","platform-id":"aggregationTypeOnPeriodAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRetentionRatePeriod1"},{"metric-id":"retention-rate","filters":{},"attribution-source":"appsflyer","aggregation-type":"on-period","granularity":"days","category":"calculated","period":"3","platform-id":"aggregationTypeOnPeriodAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRetentionRatePeriod3"},{"metric-id":"unique-users","filters":{"event-name":["app_initial_open"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNameapp_initial_openGranularityDaysMetricIdUniqueUsersPeriodLtv"},{"metric-id":"ecpa","filters":{"event-name":["app_initial_open"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNameapp_initial_openGranularityDaysMetricIdEcpaPeriodLtv"},{"metric-id":"revenue","filters":{"event-name":["app_initial_open"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNameapp_initial_openGranularityDaysMetricIdRevenuePeriodLtv"},{"metric-id":"unique-users","filters":{"event-name":["signup"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNamesignupGranularityDaysMetricIdUniqueUsersPeriodLtv"},{"metric-id":"ecpa","filters":{"event-name":["signup"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNamesignupGranularityDaysMetricIdEcpaPeriodLtv"},{"metric-id":"revenue","filters":{"event-name":["signup"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNamesignupGranularityDaysMetricIdRevenuePeriodLtv"}],"format":"json"}

CSV_DATA_PARAM = {
    "dates":{"start":"2025-10-22","end":"2025-10-22"},
    "filters":{"app-id":["com.elaworld.mexloan"]},
    "view-type":"unified",
    "localization":{"timezone":"UTC","currency":"USD"},
    "groupings":[{"dimension":"adgroup"},{"dimension":"adgroup-id"}],
    "summations":["totals","others"],
    "metrics":[
        {"metric-id":"impressions","filters":{},"granularity":"","category":"core","period":"","platform-id":"filtersGranularityMetricIdImpressionsPeriod","attribution-source":""},
        {"metric-id":"clicks","filters":{},"granularity":"","category":"core","period":"","platform-id":"filtersGranularityMetricIdClicksPeriod","attribution-source":""},
        {"metric-id":"installs","attribution-source":"appsflyer","filters":{},"granularity":"","sort-by":{"order":"desc","priority":0},"category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsPeriod"},
        {"metric-id":"installs-ua","filters":{},"attribution-source":"appsflyer","granularity":"","category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsUaPeriod"},
        {"metric-id":"installs-reattr","filters":{},"attribution-source":"appsflyer","granularity":"","category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsReattrPeriod"},
        {"metric-id":"installs-retarget","filters":{},"attribution-source":"appsflyer","granularity":"","category":"core","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdInstallsRetargetPeriod"},
        {"metric-id":"installs-cost","filters":{},"granularity":"","category":"core","period":"","platform-id":"filtersGranularityMetricIdInstallsCostPeriod","attribution-source":""},
        {"metric-id":"ecpi","filters":{},"attribution-source":"appsflyer","granularity":"","category":"calculated","period":"","platform-id":"attributionSourceAppsflyerFiltersGranularityMetricIdEcpiPeriod"},
        {"metric-id":"revenue","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"activity","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRevenuePeriodActivity"},
        {"metric-id":"revenue","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRevenuePeriodLtv"},
        {"metric-id":"roas","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"1","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRoasPeriod1"},
        {"metric-id":"roas","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"7","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRoasPeriod7"},
        {"metric-id":"roas","filters":{},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRoasPeriodLtv"},
        {"metric-id":"retention-rate","filters":{},"attribution-source":"appsflyer","aggregation-type":"on-period","granularity":"days","category":"calculated","period":"1","platform-id":"aggregationTypeOnPeriodAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRetentionRatePeriod1"},
        {"metric-id":"retention-rate","filters":{},"attribution-source":"appsflyer","aggregation-type":"on-period","granularity":"days","category":"calculated","period":"3","platform-id":"aggregationTypeOnPeriodAttributionSourceAppsflyerFiltersGranularityDaysMetricIdRetentionRatePeriod3"},
        {"metric-id":"unique-users","filters":{"event-name":["EVENT_NAME_PLACEHOLDER"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNameEVENT_NAME_PLACEHOLDERGranularityDaysMetricIdUniqueUsersPeriodLtv"},
        {"metric-id":"ecpa","filters":{"event-name":["EVENT_NAME_PLACEHOLDER"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"calculated","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNameEVENT_NAME_PLACEHOLDERGranularityDaysMetricIdEcpaPeriodLtv"},
        {"metric-id":"revenue","filters":{"event-name":["EVENT_NAME_PLACEHOLDER"]},"attribution-source":"appsflyer","aggregation-type":"cumulative","granularity":"days","category":"core","period":"ltv","platform-id":"aggregationTypeCumulativeAttributionSourceAppsflyerFilterseventNameEVENT_NAME_PLACEHOLDERGranularityDaysMetricIdRevenuePeriodLtv"}],
        "format":"csv",
        "limit":1000,
        "flat-grouping":True,
        "granularity":"days"}

# AF PRT 认证
AF_PRT_AUTH_API = "https://hq1.appsflyer.com/security-center/agency-allow-lists"
AF_PRT_PRT_VALID_API = "https://hq1.appsflyer.com/security-center/is-valid-agency?agency="