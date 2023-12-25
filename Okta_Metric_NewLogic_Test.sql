

MODULE "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsTenantProfile/Resources/v4/IDEAsTenantProfileExtension_v2.module" AS IDEAsTenantProfileExtensionModule;

#DECLARE TPIDDate DateTime = DateTime.Parse("2023-10-09"); 

#IF ("@@AggregationType@@".StartsWith("@@"))
    #DECLARE AggregationType string = "RL28"; // RL28, RL7, Daily 
#ELSE
    #DECLARE AggregationType string = "@@AggregationType@@"; // RL28, RL7, Daily 
#ENDIF
 
 
 
#IF ("@@DaysToSubtract@@".StartsWith("@@"))
    #DECLARE DaysToSubtract int = -27; // -27 for RL28, -6 for RL7, -0 for Daily 
#ELSE
    #DECLARE DaysToSubtract int = @@DaysToSubtract@@; // -27 for RL28, -6 for RL7, -0 for Daily 
#ENDIF
#IF ("@@SliceStartTime@@".StartsWith("@@"))
    #DECLARE SliceStartTime string = "2023-02-28";
#ELSE
    #DECLARE SliceStartTime string = "@@SliceStartTime@@";
#ENDIF


#DECLARE windowStart DateTime = DateTimeOffset.Parse(@SliceStartTime).DateTime;
#DECLARE windowEnd DateTime = DateTimeOffset.Parse(@SliceStartTime).DateTime;
#DECLARE Win10PlusMADSeatSize_Dtst_windowStart DateTime = new DateTimeOffset(DateTimeOffset.UtcNow.Date, TimeSpan.Zero).Add(TimeSpan.Parse("-10.00:00:00")).UtcDateTime;
#DECLARE Win10PlusMADSeatSize_Dtst_windowEnd DateTime = new DateTimeOffset(DateTimeOffset.UtcNow.Date, TimeSpan.Zero).UtcDateTime;
#DECLARE Win10PlusMADSeatSize_Dtst_lookback int = (int) Math.Round((@Win10PlusMADSeatSize_Dtst_windowEnd - @Win10PlusMADSeatSize_Dtst_windowStart).TotalDays);
#DECLARE Win10PlusMADSeatSize_Dtst_latestDate DateTime = Enumerable.Range(1, @Win10PlusMADSeatSize_Dtst_lookback).Select(x => @Win10PlusMADSeatSize_Dtst_windowEnd.AddDays(-x)).Where(x => EXISTS(string.Format("/shares/IDEAs.Prod.Data/Publish/Usage/TPID/Commercial/Metrics/Field/TenantUsageCount/SecurityAndCompliance/Win10MAD/Win10PlusMADSeatSize/Streams/v1/{0:yyyy}/{0:MM}/Win10PlusMADSeatSize_{0:yyyy}_{0:MM}_{0:dd}.ss", x))).First();
#DECLARE Win10PlusMADSeatSize_Dtst_path string = Enumerable.Range(1, @Win10PlusMADSeatSize_Dtst_lookback).Select(x => string.Format("/shares/IDEAs.Prod.Data/Publish/Usage/TPID/Commercial/Metrics/Field/TenantUsageCount/SecurityAndCompliance/Win10MAD/Win10PlusMADSeatSize/Streams/v1/{0:yyyy}/{0:MM}/Win10PlusMADSeatSize_{0:yyyy}_{0:MM}_{0:dd}.ss", @Win10PlusMADSeatSize_Dtst_windowEnd.AddDays(-x))).Where(x => EXISTS(x)).First();
#DECLARE AADAuthFed_windowStart DateTime = new DateTimeOffset(@windowStart).UtcDateTime;
#DECLARE AADAuthFed_windowEnd DateTime = new DateTimeOffset(@windowEnd).Add(TimeSpan.Parse("-00:01:00")).UtcDateTime;
#DECLARE FederationCounts_windowStart DateTime = new DateTimeOffset(@windowStart).UtcDateTime;
#DECLARE FederationCounts_windowEnd DateTime = new DateTimeOffset(@windowEnd).Add(TimeSpan.Parse("-00:01:00")).UtcDateTime;
#DECLARE IDEAsTenantProfileExtension_windowStart DateTime = new DateTimeOffset(@windowStart).UtcDateTime;
#DECLARE IDEAsTenantProfileExtension_windowEnd DateTime = new DateTimeOffset(@windowEnd).Add(TimeSpan.Parse("-00:01:00")).UtcDateTime;
#DECLARE Compete_AccAuthDetectionsEnr_FY24_Dtst_outputPath string = string.Format("/local/users/beca/Okta/1009/FY24_AccAuthDetectionsEnriched_{0:yyyy}_{0:MM}_{0:dd}.ss", @windowStart);
#DECLARE Compete_AccAuthDetectionsEnr_FY24_Dtst_streamExpiry string = TimeSpan.FromTicks(Math.Max(TimeSpan.FromDays(28).Ticks, (@windowEnd.AddDays(547).Add(TimeSpan.Parse("5.00:00:00")) - DateTime.UtcNow).Ticks)).ToString();
#DECLARE Compete_AccAuthDetectionsAgg_FY24_Dtst_outputPath string = string.Format("/local/users/beca/Okta/1009/FY2_4AccAuthDetectionsAggregated_{0:yyyy}_{0:MM}_{0:dd}.ss", @windowStart);
#DECLARE Compete_AccAuthDetectionsAgg_FY24_Dtst_streamExpiry string = TimeSpan.FromTicks(Math.Max(TimeSpan.FromDays(28).Ticks, (@windowEnd.AddDays(547).Add(TimeSpan.Parse("5.00:00:00")) - DateTime.UtcNow).Ticks)).ToString();


#DECLARE DetectionDate DateTime = DateTime.Parse("2023-11-30");

#DECLARE detection string = string.Format("local/users/beca/Okta/ImprovedDetection_{0}_{1}_{2}.ss", @DetectionDate.Year.ToString(), @DetectionDate.Month.ToString("D2"), @DetectionDate.Day.ToString("D2"));
detection = SSTREAM @detection;

Compete_AccAuthDetectionsEnriched_TAU =
    SELECT *,
           true AS IsTPIDOkta
    FROM detection;


Win10PlusMADSeatSize_Dtst =
    SELECT TPID,
           Seatsize,
           SeatsizeKey
    FROM
    (
        SSTREAM @Win10PlusMADSeatSize_Dtst_path
    );

//Compete_AccAuthDetectionsEnriched_TAU =
//    SELECT TPID,
//           GPID,
//           SegmentName,
//           SubSegmentName,
//           SegmentGroup,
//           AreaName,
//           RegionName,
//           IndustryName,
//           VerticalName,
//           IsMAL,
//           IsActive,
//           IsTPIDOkta
//    FROM
//    (
//        VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/TPID/Commercial/Metrics/Field/TenantUsageCount/SecurityAndCompliance/Okta/Intermediate/AccAuthDetectionsEnriched/Views/v2/AccAuthDetectionsEnriched.view"
//        PARAMS
//        (
//            HistoryDate = @windowStart
//        )
//    );

Compete_TenantTPIDSegmentation =
    SELECT TenantId,
           TPID,
           IsStrategicCustomer,
           SubSegmentName,
           SegmentName,
           SegmentGroup,
           AreaName,
           GeoLabel,
           BigAreaName,
           IndustryName,
           VerticalName,
           IsAADMAU1Plus,
           IsAADMAU500Plus,
           IsTPIDA310,
           IsTPIDA210,
           IsTPIDA110,
           IsTPIDA010,
           IsMAL,
           IsEDU,
           IsSMB,
           IsActive,
           IsS2500,
           IsS500
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/TPID/Commercial/CompeteMetrics/FY24/SourceInterfaces/CompeteTenantTPIDSegmentation/Views/v2/Compete_TenantTPIDSegmentation.view"
        PARAMS
        (
            AsOfDate = @windowStart
        )
    );

#IF(new DateTimeOffset(@windowStart) >= DateTimeOffset.Parse("2023-09-07T00:00:00Z"))
    AADAuthFed =
        SELECT FactDate,
               TenantId,
               AuthType,
               StsProduct,
               ConfiguredUserCount,
               UniqueActiveUserCount,
               SuccessfulAuthCount
        FROM
        (
            VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/User/Commercial/ActionView/AADAuthFed/Views/v1/AADAuthFed.view"
            PARAMS
            (
                SnapshotTime = @AADAuthFed_windowStart
            )
        );
#ELSE
    AADAuthFed =
        SELECT default(DateTime) AS FactDate,
               default(string) AS TenantId,
               default(string) AS AuthType,
               default(string) AS StsProduct,
               default(long?) AS ConfiguredUserCount,
               default(long) AS UniqueActiveUserCount,
               default(long?) AS SuccessfulAuthCount
        FROM
        (
            VALUES(0)
        ) AS _
        WHERE false;
#ENDIF

AADAuthMeasure =
    SELECT TPID,
           CustomerSegmentGroup,
           AADLoginMAU,
           IsAAD,
           IsAAD500
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/TPID/Commercial/CompeteMetrics/MetricsEnriched/AccAuthDetectionsEnriched/Views/v1/AADAuthMeasureInterface.view"
        PARAMS
        (
            Date = @windowStart
        )
    );

AADIncentive =
    SELECT TPID,
           CustomerSegmentGroup,
           AADIncentiveMAU,
           IsAADIncentive
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/TPID/Commercial/CompeteMetrics/MetricsEnriched/AccAuthDetectionsEnriched/Views/v1/AADPIncentiveInterface.view"
        PARAMS
        (
            Date = @windowStart
        )
    );

AADPMAU =
    SELECT TPID,
           CustomerSegmentGroup,
           AADPMAU,
           IsAADP
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/TPID/Commercial/CompeteMetrics/MetricsEnriched/AccAuthDetectionsEnriched/Views/v1/AADPMAUInterface.view"
        PARAMS
        (
            Date = @windowStart
        )
    );

#IF(new DateTimeOffset(@windowStart) < DateTimeOffset.Parse("2023-09-07T00:00:00Z"))
    FederationCounts =
        SELECT FactDate,
               ContextId,
               AuthType,
               StsProduct,
               ConfiguredUserCount,
               UniqueActiveUserCount,
               SuccessfulAuthCount
        FROM
        (
            VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/User/Neutral/Reporting/ExternalViews/FederationCounts/Views/v1/FederationCounts.view"
            PARAMS
            (
                EndDate = @FederationCounts_windowStart
            )
        );
#ELSE
    FederationCounts =
        SELECT default(DateTime) AS FactDate,
               default(Guid?) AS ContextId,
               default(string) AS AuthType,
               default(string) AS StsProduct,
               default(long) AS ConfiguredUserCount,
               default(long) AS UniqueActiveUserCount,
               default(long) AS SuccessfulAuthCount
        FROM
        (
            VALUES(0)
        ) AS _
        WHERE false;
#ENDIF

FortuneFlagsProfile =
    SELECT TPID,
           IsF1000,
           IsF500,
           IsG500
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsFortuneFlagsProfile/Views/v1/IDEAsFortuneFlagsProfile.view"
    );

IDEAsCalcProfile =
    SELECT TPID,
           Segment,
           SubSegment,
           SegmentGroup,
           Industry,
           Vertical,
           AreaName,
           RegionName,
           GPID,
           HQDS,
           AccountStatus
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsCalcProfile/Views/v1/IDEAsCalcProfile.view"
    );

IDEAsTenantsProfile =
    SELECT TenantId,
           MSSalesTopParentOrgId,
           CustomerSegmentGroup
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/Metrics/TenantProperties/Views/v1/TenantProperties_GoLive.view"
        PARAMS
        (
            HistoryDate = @TPIDDate
        )
    );

TAUAADP =
    SELECT OmsTenantId,
           AllUp
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/TenantActiveUsage/AADP/Views/v4/AADP_TenantActiveUsage.view"
        PARAMS
        (
            InputDate = @windowStart,
            Application = "AADP",
            AggregationType = "RL28",
            EntityType = "User",
            SubWorkload = "AllUp",
            Workload = "AllUp",
            Feature = "SaasLogin"
        )
    );

TPIDAttributes =
    SELECT TPID,
           TPName,
           SegmentName,
           SubSegmentName,
           SegmentGroup,
           BigAreaName,
           AreaName,
           SubsidiaryName,
           RegionName,
           SubRegionName,
           CountryName,
           IsEDU,
           IsSMB,
           IsMAL,
           Industry,
           Vertical,
           AccountStatus
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Profiles/TPID/Commercial/Metrics/TPIDAttributes/Views/v1/TPIDAttributes_GoLive.view"
        PARAMS
        (
            HistoryDate = @windowStart
        )
    );

IDEAsTenantProfileExtension =
    SELECT IDEAsMSSales_v3_MSSalesTopParentOrgId,
           IDEAsPAU_v3_AADPP1,
           IDEAsPAU_v3_AADPP2,
           IDEAsPAU_v3_AADP,
           IsMSODSDeleted,
           State,
           IDEAsInternal_v4_IsTest,
           IDEAsInternal_v4_FraudClassification,
           OMSTenantId,
           IDEAsTenantCloudType_v2_TenantCloudType
    FROM IDEAsTenantProfileExtensionModule.IDEAsTenantProfileExtensionView
    (
        HistoryDate = @IDEAsTenantProfileExtension_windowStart,
        IDEAsPAU_v3_HistoryDate = @IDEAsTenantProfileExtension_windowStart,
        IDEAsCALC_v3_HistoryDate = @IDEAsTenantProfileExtension_windowStart,
        IDEAsInternal_v4_HistoryDate = @IDEAsTenantProfileExtension_windowStart,
        IDEAsMSSales_v3_HistoryDate = @IDEAsTenantProfileExtension_windowStart,
        Extensions = new ARRAY<string> { "IDEAsMSSales", "IDEAsCALC", "IDEAsInternal", "IDEAsPAU", "IDEAsTenantCloudType" }
    );

TenantProfileExt_Restate_Curr =
    SELECT IDEAsMSSales_v3_MSSalesTopParentOrgId,
           IDEAsMSSales_v3_MSSalesTopParentOrgName,
           IDEAsFastTrackTenants_v4_IsLatestFastTrackStrategic,
           IDEAsMSSales_v3_MSSalesIndustrySummary,
           IDEAsInternal_v4_IsMSFTTenant
    FROM IDEAsTenantProfileExtensionModule.IDEAsTenantProfileExtensionView
    (
        HistoryDate = @windowStart,
        IDEAsInternal_v4_HistoryDate = @windowStart,
        IDEAsMSSales_v3_HistoryDate = @windowStart,
        IDEAsFastTrackTenants_v4_HistoryDate = @windowStart,
        Extensions = new ARRAY<string> { "IDEAsMSSales", "IDEAsInternal", "IDEAsFastTrackTenants" }
    );

// START BUSINESS LOGIC. Edit below this line

USING System.Security.Cryptography;

IDEAsTenantsProfile =
    SELECT TenantId AS OMSTenantId,
           MSSalesTopParentOrgId,
           CustomerSegmentGroup
    FROM IDEAsTenantsProfile;

TenantTPId =
    SELECT
        OMSTenantId.ToUpper() AS TenantId,
        MSSalesTopParentOrgId AS TPId,
        CustomerSegmentGroup
    FROM IDEAsTenantsProfile;

PAU =
    SELECT DISTINCT
        @windowStart AS Date,
        IDEAsMSSales_v3_MSSalesTopParentOrgId AS TpId,
        SUM (IDEAsPAU_v3_AADPP1) AS AADP1PAU,
        SUM (IDEAsPAU_v3_AADPP2) AS AADP2PAU,
        SUM (IDEAsPAU_v3_AADP) AS AADPPAU
    FROM IDEAsTenantProfileExtension
    WHERE 1==1
    AND IsMSODSDeleted == "False"
    AND State != "Deleted"
    AND IDEAsInternal_v4_IsTest == false
    AND IDEAsInternal_v4_FraudClassification == false
    AND !String.IsNullOrEmpty(OMSTenantId)
    AND IDEAsPAU_v3_AADP >0;

PAU_AADP_FLAGS =
    SELECT  Date,
            TpId AS TPID,
            AADP1PAU,
            AADP2PAU,
            AADPPAU,
            //New Flag
            IF (AADP1PAU >0, true, false) AS IsAADP1PAU,
            IF (AADP2PAU >0, true, false) AS IsAADP2PAU,
            IF (AADPPAU >0, true, false) AS IsAADPPAU,
            IF (AADP1PAU >=25, true, false) AS IsAADP1PAU25,
            IF (AADP2PAU >=25, true, false) AS IsAADP2PAU25,
            IF (AADPPAU >=25, true, false) AS IsAADPPAU25,
            IF (AADP1PAU >=500, true, false) AS IsAADP1PAU500,
            IF (AADP2PAU >=500, true, false) AS IsAADP2PAU500,
            IF (AADPPAU >=500, true, false) AS IsAADPPAU500,
            IF (AADP1PAU >=250, true, false) AS IsAADP1PAU250,
            IF (AADP2PAU >=250, true, false) AS IsAADP2PAU250,
            IF (AADPPAU >=250, true, false) AS IsAADPPAU250
    FROM PAU;

#IF(new DateTimeOffset(@windowStart) >= DateTimeOffset.Parse("2023-09-07T00:00:00Z").UtcDateTime)
    FederationCounts =  SELECT *.Except(TenantId), TenantId.ToUpper() AS TenantId
                    FROM AADAuthFed;
#ELSE
    FederationCounts =  SELECT *.Except(ContextId), ContextId.ToString().ToUpper() AS TenantId
                    FROM FederationCounts;
#ENDIF

FederationCounts =
    SELECT TPId,
           IF(AuthType != "Federated", AuthType, StsProduct) AS AuthProduct,
           SUM(IF(UniqueActiveUserCount IS NULL, 0, UniqueActiveUserCount)) AS AADMAU
    FROM FederationCounts AS a
         LEFT OUTER JOIN
             TenantTPId
         ON a.TenantId == TenantTPId.TenantId
    HAVING AADMAU > 0;

FederationCounts =
     SELECT TPId,
            SUM(AADMAU) AS AADMAU,
            SUM(IF (AuthProduct IN ("PHS","PTA","Cloud Only" ), AADMAU, 0)) AS NativeAADMAU,
            SUM(IF (AuthProduct == "ADFS",AADMAU, 0)) AS ADFSMAU,
            SUM(IF (AuthProduct == "Ping",AADMAU, 0)) AS PingMAU,
            SUM(IF (AuthProduct == "Okta",AADMAU, 0)) AS OktaMAU,
            SUM(IF (AuthProduct == "HDEMS",AADMAU, 0)) AS HDEMSMAU
     FROM FederationCounts;

FederationCounts =
     SELECT TPId AS TPID,
            IF (NativeAADMAU > 0, true, false) AS IsNativeAAD,
            IF (ADFSMAU > 0, true, false) AS IsADFS,
            IF (PingMAU > 0, true, false) AS IsPing,
            IF (OktaMAU > 0, true, false) AS IsOkta,
            IF (HDEMSMAU > 0, true, false) AS IsHDEMS,
            AADMAU,
            NativeAADMAU,
            ADFSMAU,
            PingMAU,
            OktaMAU,
            HDEMSMAU
     FROM FederationCounts;

IDEAsTenantProfileExt_Restate =
    SELECT
        IDEAsMSSales_v3_MSSalesTopParentOrgId,
        IDEAsMSSales_v3_MSSalesTopParentOrgName,
        IDEAsFastTrackTenants_v4_IsLatestFastTrackStrategic,
        IDEAsMSSales_v3_MSSalesIndustrySummary,
        IDEAsInternal_v4_IsMSFTTenant
    FROM TenantProfileExt_Restate_Curr;

IDEAsTenantProfileExt_Restate =
   SELECT
        IDEAsMSSales_v3_MSSalesTopParentOrgId AS Tpid,
        IDEAsMSSales_v3_MSSalesTopParentOrgName AS TPName,
        MAX(IDEAsFastTrackTenants_v4_IsLatestFastTrackStrategic) AS IsS2500,
        string.Empty AS IDEAsSegment, //placeholder until data is ready
        string.Empty AS IDEAsSegmentGroup, //placeholder until data is ready
        MAX(IDEAsMSSales_v3_MSSalesIndustrySummary) AS IndustrySummary,
        MAX(IDEAsInternal_v4_IsMSFTTenant) AS IsMSFTTenant,
        COUNT(1) AS ct
    FROM
    IDEAsTenantProfileExt_Restate;

TPID_IsS500 =
    SELECT
        TPID,
        MAX(IsS500) AS IsS500,
        COUNT(1) AS Dedup
    FROM Compete_TenantTPIDSegmentation;

TPIDA310 =
    SELECT
        @windowStart AS AsOfDate,
        -1 AS TPID,
        0 AS AADUniqueLogins,
        false AS IsAADMAU1Plus,
        false AS IsAADMAU500Plus,
        false AS IsTPIDMAUQualified,
        false AS IsTPIDA310,
        false AS IsTPIDA210,
        false AS IsTPIDA110,
        false AS IsTPIDA010
    FROM
        (
            VALUES(0)
        ) AS _
        WHERE false;

CalcHQ =
    SELECT *
    FROM IDEAsCalcProfile
    WHERE HQDS == "HQ";

TAUAADP =
    SELECT
        OmsTenantId.ToUpper() AS TenantId,
        AllUp AS SaaSMAU
    FROM TAUAADP;

SaaSMAU =
    SELECT
        TPId,
        CustomerSegmentGroup,
        SaaSMAU,
        SaaSMAU > 0 ? true: false AS IsSaaS
    FROM
    (
        SELECT
            TPId,
            CustomerSegmentGroup,
            SUM(SaaSMAU) AS SaaSMAU
        FROM TAUAADP
        LEFT OUTER JOIN TenantTPId
            ON TAUAADP.TenantId == TenantTPId.TenantId
    );

Detections =
    SELECT DISTINCT
        IF(CalcHQ.GPID == NULL, TPIDProf.TPID, CalcHQ.TPID) AS TPID,
        //IF(CalcHQ.GPID == NULL, TPIDProf.SubSegmentName, CalcHQ.SubSegment) AS SubSegment,
        //IF(CalcHQ.GPID == NULL, TPIDProf.SegmentName, CalcHQ.Segment) AS SegmentName,
        //IF(CalcHQ.GPID == NULL, TPIDProf.SegmentGroup, CalcHQ.SegmentGroup) AS SegmentGroup,
        //IF(CalcHQ.GPID == NULL, TPIDProf.IndustryName, CalcHQ.Industry) AS IndustryName,
        //IF(CalcHQ.GPID == NULL, TPIDProf.VerticalName, CalcHQ.Vertical) AS VerticalName,
        //IF(CalcHQ.GPID == NULL, TPIDProf.AreaName, CalcHQ.AreaName) AS AreaName,
        //IF(CalcHQ.GPID == NULL, TPIDProf.RegionName, CalcHQ.RegionName) AS RegionName,
        //TPIDProf.IsMAL,
        //TPIDProf.IsActive,
        //false AS IsEDU,
        //false AS IsSMB,
        MAX(TPIDProf.IsTPIDOkta) AS IsTPIDOkta
    FROM Compete_AccAuthDetectionsEnriched_TAU AS TPIDProf
        LEFT OUTER JOIN CalcHQ
            ON TPIDProf.TPID == CalcHQ.TPID;

AllAADMeasure =
    SELECT
        A.TPId,
        A.CustomerSegmentGroup,
        AADAuthMeasure.AADLoginMAU,
        AADAuthMeasure.IsAAD,
        AADAuthMeasure.IsAAD500,
        SaaSMAU.SaaSMAU,
        SaaSMAU.IsSaaS,
        AADIncentive.AADIncentiveMAU,
        AADIncentive.IsAADIncentive,
        AADPMAU.AADPMAU,
        AADPMAU.IsAADP
    FROM (SELECT TPId, MIN(CustomerSegmentGroup) AS CustomerSegmentGroup FROM TenantTPId) AS A
        LEFT OUTER JOIN AADAuthMeasure
            ON A.TPId == AADAuthMeasure.TPID
        LEFT OUTER JOIN SaaSMAU
            ON A.TPId == SaaSMAU.TPId
        LEFT OUTER JOIN AADIncentive
            ON A.TPId == AADIncentive.TPID
        LEFT OUTER JOIN AADPMAU
            ON A.TPId == AADPMAU.TPID;

//AllMeasurePre =
//    SELECT
//        1 AS IsOktaPipe, // Somehow, AAD measures have Enterprise and SMC TPIds that are not part of this dataset, thus changing the TotalAccoutns measure. For now bypassing this with a flag, but need further investigation and rationalizing with Data Analyst
//        A.TPID,
//        B.CustomerSegmentGroup,
//        (bool?)A.IsMAL AS IsMAL,
//        (bool?)A.IsActive AS IsActive,
//        (bool?)A.IsEDU AS IsEDU,
//        (bool?)A.IsSMB AS IsSMB,
//        (bool?)A.IsTPIDOkta AS IsTPIDOkta,
//        B.AADLoginMAU,
//        B.IsAAD,
//        B.IsAAD500,
//        B.SaaSMAU,
//        B.IsSaaS,
//        B.AADIncentiveMAU,
//        B.IsAADIncentive,
//        B.AADPMAU,
//        B.IsAADP
//    FROM Detections AS A
//    LEFT OUTER JOIN AllAADMeasure AS B
//        ON A.TPID == B.TPId;

AllMeasure =
//    SELECT * FROM AllMeasurePre
//    UNION ALL BY NAME
    SELECT
        1 AS IsOktaPipe,
        A.TPId AS TPID,
        A.CustomerSegmentGroup,
//        (bool?)B.IsMAL AS IsMAL,
//        (bool?)B.IsActive AS IsActive,
//        (bool?)B.IsEDU AS IsEDU,
//        (bool?)B.IsSMB AS IsSMB,
//        (bool?)null AS IsTPIDOkta,
        IF(B.TPID == null, false, true) AS IsTPIDOkta,
        A.AADLoginMAU,
        A.IsAAD,
        A.IsAAD500,
        A.SaaSMAU,
        A.IsSaaS,
        A.AADIncentiveMAU,
        A.IsAADIncentive,
        A.AADPMAU,
        A.IsAADP
    FROM AllAADMeasure AS A
    LEFT OUTER JOIN Detections AS B
        ON A.TPId == B.TPID
//    WHERE B.TPID == null
;

Compete_AccAuthDetectionsEnr_FY24_Dtst =
    SELECT DISTINCT
        @windowStart AS AsOfDate,
        A.TPID,
        (long?) A.TPID AS TPID_Long,
        ER.TPName,
        A.CustomerSegmentGroup,
//        IsMAL,
//        IsActive,
//        IsEDU,
//        IsSMB,
        IsOktaPipe,
        IsOktaPipe == 1? TPIDA310.AADUniqueLogins : 0 AS AAD_UniqueLogins,
        IsOktaPipe == 1? TPIDA310.IsAADMAU1Plus : (bool?)null AS IsAADMAU1Plus,
        IsOktaPipe == 1? TPIDA310.IsAADMAU500Plus : (bool?)null AS IsAADMAU500Plus,
        IsOktaPipe == 1? TPIDA310.IsTPIDMAUQualified : (bool?)null AS IsTPIDMAUQualified,
        IsOktaPipe == 1? TPIDA310.IsTPIDA310 : (bool?)null AS IsTPIDA310,
        IsOktaPipe == 1? IF(TPIDA310.IsTPIDA310 == true OR TPIDA310.IsTPIDA210 == true, true, false) : (bool?)null AS IsTPIDA210,
        IsOktaPipe == 1? IF(TPIDA310.IsTPIDA310 == true OR TPIDA310.IsTPIDA210 == true OR TPIDA310.IsTPIDA110 == true, true, false) : (bool?)null AS IsTPIDA110,
        IsOktaPipe == 1? IF(TPIDA310.IsTPIDA310 == true OR TPIDA310.IsTPIDA210 == true OR TPIDA310.IsTPIDA110 == true OR IsTPIDA010 == true, true, false) : (bool?)null AS IsTPIDA010,
        IsTPIDOkta,
        IsOktaPipe == 1? A.TPID : null AS TotalAccTPID,
        ER.IsS2500,
        ER.IDEAsSegment,
        ER.IDEAsSegmentGroup,
        ER.IndustrySummary,
        ER.IsMSFTTenant,
        A.AADLoginMAU,
        A.IsAAD,
        A.IsAAD500,
        A.SaaSMAU,
        A.IsSaaS,
        A.AADIncentiveMAU,
        A.IsAADIncentive,
        A.AADPMAU,
        A.IsAADP
    FROM AllMeasure AS A
        FULL OUTER JOIN TPIDA310
            ON A.TPID == TPIDA310.TPID
        LEFT JOIN IDEAsTenantProfileExt_Restate AS ER
            ON A.TPID == ER.Tpid;

Compete_AccAuthDetectionsEnr_FY24_Dtst =
    SELECT
        Enriched.AsOfDate,
        Enriched.TPID,
        Enriched.TPName,
        Enriched.TotalAccTPID,
        Enriched.IsOktaPipe,
        Enriched.IsOktaPipe AS IsOktaPipe2,
        Enriched.CustomerSegmentGroup,
        TPID.SubSegmentName,
        TPID.SegmentName,
        TPID.SegmentGroup,
        IF (TPID.IsEDU == true, "EDU",
        IF (TPID.SegmentGroup == "Enterprise", "Enterprise",
        IF (TPID.SegmentName != NULL AND TPID.SegmentName.ToLower().Contains("scale"),"SMC Scale",
        IF (SegmentGroup == "SMC Corporate", "SMC Managed",
        IF (Enriched.CustomerSegmentGroup == "SMC - SMB", "SMB", "Others"))))) AS SegmentDerived,
        Enriched.IDEAsSegment,
        Enriched.IDEAsSegmentGroup,
        TPID.Industry AS IndustryName,
        Enriched.IndustrySummary,
        Enriched.IsMSFTTenant,
        TPID.Vertical AS VerticalName,
        TPID.AreaName,
        TPID.RegionName,
        IF(TPID.AreaName IN("ANZ", "Canada", "France", "Germany", "Japan", "UK", "United States", "Western Europe"), "Field Metric Geo", "Other Geo") AS GeoLabel,
        TPID.IsMAL,
        TPID.AccountStatus == "Active"? true:false AS IsActive,
        TPID.IsEDU,
        TPID.IsSMB,
        Enriched.AAD_UniqueLogins,
        Enriched.IsAADMAU1Plus,
        Enriched.IsAADMAU500Plus,
        Enriched.IsTPIDMAUQualified,
        Enriched.IsTPIDA310,
        Enriched.IsTPIDA210,
        Enriched.IsTPIDA110,
        Enriched.IsTPIDA010,
        Enriched.IsTPIDOkta,
        Enriched.IsS2500,
        S500.IsS500,
        Enriched.AADLoginMAU,
        Enriched.IsAAD,
        Enriched.IsAAD500,
        Enriched.SaaSMAU,
        Enriched.IsSaaS,
        Enriched.AADIncentiveMAU,
        Enriched.IsAADIncentive,
        Enriched.AADPMAU,
        Enriched.IsAADP,
        FortuneFlags.IsF500,
        FortuneFlags.IsF1000,
        FortuneFlags.IsG500,
        string.Empty AS FortuneSector,
        (string.IsNullOrWhiteSpace(Seatsize) ? string.Empty : Seatsize) AS Seatsize,
        (SeatsizeKey == null ? 9999L : SeatsizeKey) AS SeatsizeKey,
        string.Empty AS Platform,
        string.Empty AS ClientOSName,
        string.Empty AS Os_Group,
        PAU.IsAADPPAU,
        PAU.IsAADPPAU25,
        PAU.IsAADPPAU500,
        PAU.IsAADPPAU250,
        PAU.IsAADP1PAU,
        PAU.IsAADP2PAU,
        PAU.IsAADP1PAU25,
        PAU.IsAADP2PAU25,
        PAU.IsAADP1PAU500,
        PAU.IsAADP2PAU500,
        PAU.IsAADP1PAU250,
        PAU.IsAADP2PAU250,
        PAU.AADP1PAU,
        PAU.AADP2PAU,
        PAU.AADPPAU,
        IF(F.IsNativeAAD == true, true, false) AS IsNativeAAD,
        IF(F.IsADFS == true, true, false) AS IsADFS,
        IF(F.IsOkta == true, true, false) AS IsOkta,
        IF(F.IsPing == true, true, false) AS IsPing,
        IF(F.IsHDEMS == true, true, false) AS IsHDEMS,
        MAX(IF(F.TPID == null,false, true))AS IsAADNewSource,
        SUM(F.AADMAU) AS AADMAU,
        SUM(F.NativeAADMAU) AS NativeAADMAU,
        SUM(F.ADFSMAU) AS ADFSMAU,
        SUM(F.PingMAU) AS PingMAU,
        SUM(F.OktaMAU) AS OktaMAU,
        SUM(F.HDEMSMAU) AS HDEMSMAU
    FROM Compete_AccAuthDetectionsEnr_FY24_Dtst AS Enriched
        LEFT OUTER JOIN TPIDAttributes AS TPID
            ON Enriched.TPID == TPID.TPID
        LEFT OUTER JOIN TPID_IsS500 AS S500
            ON Enriched.TPID == S500.TPID
        LEFT OUTER JOIN FortuneFlagsProfile AS FortuneFlags
            ON Enriched.TPID == FortuneFlags.TPID
        LEFT OUTER JOIN PAU_AADP_FLAGS AS PAU
            ON Enriched.TPID == PAU.TPID
        LEFT OUTER JOIN FederationCounts AS F
            ON Enriched.TPID == F.TPID
        LEFT OUTER JOIN Win10PlusMADSeatSize_Dtst
            ON Enriched.TPID_Long == Win10PlusMADSeatSize_Dtst.TPID;

Compete_AccAuthDetectionsAgg_FY24_Dtst =
    SELECT
        AsOfDate,
        BitConverter.ToInt64(MD5.Create().ComputeHash(Encoding.ASCII.GetBytes(" " + ClientOSName + " " + FortuneSector + " " + Platform  )), 0) AS OtherAttributesKey,
        IsOktaPipe,
        IsOktaPipe2,
        AsOfDateKey,
        SegmentKey,
        CustomerSegmentGroup,
        SubSegmentName,
        SegmentName,
        SegmentGroup,
        SegmentDerived,
        IDEAsSegment,
        IDEAsSegmentGroup,
        GeoKey,
        GeoLabel,
        AreaName,
        IndustryKey,
        IndustryName,
        IndustrySummary,
        IsMSFTTenant,
        VerticalName,
        BitConverter.ToInt64(MD5.Create().ComputeHash(Encoding.ASCII.GetBytes(
        " " + IndustryName + IsActive + IsTPIDMAUQualified + IsMAL + IsEDU + IsSMB + IsS500 + IsS2500 + IsF500 + IsF1000 + IsG500 + IsTPIDA310 + IsTPIDA210 + IsTPIDA110 + IsTPIDA010 + IsTPIDOkta + IsAADMAU1Plus + IsAADMAU500Plus + IsAAD + IsAAD500 + IsSaaS + IsAADIncentive + IsAADP + IsMSFTTenant
        )), 0) AS TPIDAttributesKey,
        IsMAL,
        IsEDU,
        IsSMB,
        IsActive,
        IsS2500,
        IsS500,
        IsAADMAU1Plus,
        IsAADMAU500Plus,
        IsTPIDMAUQualified,
        IsTPIDA310,
        IsTPIDA210,
        IsTPIDA110,
        IsTPIDA010,
        IsTPIDOkta,
        TotalAccounts,
        TotalAllAccounts,
        AADMAU1PlusAccounts,
        AADMAU500PlusAccounts,
        QualifiedAccounts,
        A310Accounts,
        A210Accounts,
        A110Accounts,
        A010Accounts,
        OktaDetectedAccounts,
        SaasAccounts,
        AADPAccounts,
        AADAccounts,
        AADIncentiveAccounts,
        AADLoginMAU,
        IsAAD,
        IsAAD500,
        SaaSMAU,
        IsSaaS,
        AADIncentiveMAU,
        IsAADIncentive,
        AADPMAU,
        IsAADP,
        IsF500,
        IsF1000,
        IsG500,
        FortuneSector,
        Seatsize,
        SeatsizeKey,
        Platform,
        ClientOSName,
        Os_Group,
        IsAADPPAU,
        IsAADPPAU25,
        IsAADPPAU500,
        IsAADPPAU250,
        AADP1PAU,
        AADP2PAU,
        AADPPAU,
        AADPPaidAllAccounts,
        AADPPaid25AllAccounts,
        AADPPaid250AllAccounts,
        AADPPaid500AllAccounts,
        AADP1PaidAllAccounts,
        AADP1Paid25AllAccounts,
        AADP1Paid250AllAccounts,
        AADP1Paid500AllAccounts,
        AADP2PaidAllAccounts,
        AADP2Paid25AllAccounts,
        AADP2Paid250AllAccounts,
        AADP2Paid500AllAccounts,
        AADPPaidAccounts,
        AADPPaid25Accounts,
        AADPPaid250Accounts,
        AADPPaid500Accounts,
        AADP1PaidAccounts,
        AADP1Paid25Accounts,
        AADP1Paid250Accounts,
        AADP1Paid500Accounts,
        AADP2PaidAccounts,
        AADP2Paid25Accounts,
        AADP2Paid250Accounts,
        AADP2Paid500Accounts,
        AADNewSourceAccounts,
        NativeAADAccounts,
        IsAADNewSource,
        IsNativeAAD,
        IsADFS,
        IsOkta,
        IsPing,
        IsHDEMS,
        ADFSAccounts,
        OktaAccounts,
        PingAccounts,
        HDEMSAccounts,
        AADMAU,
        NativeAADMAU,
        ADFSMAU,
        PingMAU,
        OktaMAU,
        HDEMSMAU,
        AADNewSourceAllAccounts,
        NativeAADAllAccounts,
        ADFSAllAccounts,
        OktaAllAccounts,
        PingAllAccounts,
        HDEMSAllAccounts
    FROM
    (
        SELECT DISTINCT
            AsOfDate,
            IsOktaPipe,
            IsOktaPipe2,
            CustomerSegmentGroup,
            int.Parse(AsOfDate.ToString("yyyyMMdd")) AS AsOfDateKey,
            BitConverter.ToInt64(MD5.Create().ComputeHash(Encoding.ASCII.GetBytes(" " + CustomerSegmentGroup + SubSegmentName + SegmentName + SegmentGroup + SegmentDerived + IDEAsSegment + IDEAsSegmentGroup)), 0) AS SegmentKey,
            SubSegmentName,
            SegmentName,
            SegmentGroup,
            IDEAsSegment,
            IDEAsSegmentGroup,
            SegmentDerived,
            BitConverter.ToInt64(MD5.Create().ComputeHash(Encoding.ASCII.GetBytes(" " + GeoLabel + AreaName)), 0) AS GeoKey,
            GeoLabel,
            AreaName,
            BitConverter.ToInt64(MD5.Create().ComputeHash(Encoding.ASCII.GetBytes(" " + IndustryName + IndustrySummary + VerticalName)), 0) AS IndustryKey,
            IndustryName,
            IndustrySummary,
            IsMSFTTenant,
            VerticalName,
            IsMAL == null ? false : IsMAL AS IsMAL,
            IsEDU == null ? false : IsEDU AS IsEDU,
            IsSMB == null ? false : IsSMB AS IsSMB,
            IsActive == null ? false : IsActive AS IsActive,
            IsS2500 == null ? false : IsS2500 AS IsS2500,
            IsS500 == null ? false : IsS500 AS IsS500,
            IsTPIDMAUQualified == null ? false : IsTPIDMAUQualified AS IsTPIDMAUQualified,
            IsTPIDA310 == null ? false : IsTPIDA310 AS IsTPIDA310,
            IsTPIDA210 == null ? false : IsTPIDA210 AS IsTPIDA210,
            IsTPIDA110 == null ? false : IsTPIDA110 AS IsTPIDA110,
            IsTPIDA010 == null ? false : IsTPIDA010 AS IsTPIDA010,
            IsAADMAU1Plus == null ? false : IsAADMAU1Plus AS IsAADMAU1Plus,
            IsAADMAU500Plus == null ? false : IsAADMAU500Plus AS IsAADMAU500Plus,
            IsTPIDOkta == null ? false : IsTPIDOkta AS IsTPIDOkta,
            COUNT(DISTINCT TotalAccTPID) AS TotalAccounts,
            COUNT (DISTINCT TPID) AS TotalAllAccounts,
            COUNT(DISTINCT (IsAADMAU1Plus == true? TPID : null)) AS AADMAU1PlusAccounts,
            COUNT(DISTINCT (IsAADMAU500Plus == true? TPID : null)) AS AADMAU500PlusAccounts,
            COUNT(DISTINCT (IsTPIDMAUQualified == true? TPID : null)) AS QualifiedAccounts,
            COUNT(DISTINCT (IsTPIDA310 == true? TPID : null)) AS A310Accounts,
            COUNT(DISTINCT (IsTPIDA210 == true? TPID : null)) AS A210Accounts,
            COUNT(DISTINCT (IsTPIDA110 == true? TPID : null)) AS A110Accounts,
            COUNT(DISTINCT (IsTPIDA010 == true? TPID : null)) AS A010Accounts,
            COUNT(DISTINCT (IsTPIDOkta == true? TPID : null)) AS OktaDetectedAccounts,
            COUNT(DISTINCT (IsSaaS == true? TotalAccTPID : null)) AS SaasAccounts,
            COUNT(DISTINCT (IsAADP == true? TotalAccTPID : null)) AS AADPAccounts,
            COUNT(DISTINCT (IsAAD == true? TotalAccTPID : null)) AS AADAccounts,
            COUNT(DISTINCT (IsAADIncentive == true? TotalAccTPID : null)) AS AADIncentiveAccounts,
            SUM(AADLoginMAU) AS AADLoginMAU,
            IsAAD == null ? false : IsAAD AS IsAAD,
            IsAAD500 == null ? false : IsAAD500 AS IsAAD500,
            SUM(SaaSMAU) AS SaaSMAU,
            IsSaaS == null ? false : IsSaaS AS IsSaaS,
            SUM(AADIncentiveMAU) AS AADIncentiveMAU,
            IsAADIncentive == null ? false : IsAADIncentive AS IsAADIncentive,
            SUM(AADPMAU) AS AADPMAU,
            IsAADP == null ? false : IsAADP AS IsAADP,
            IsF500,
            IsF1000,
            IsG500,
            FortuneSector,
            Seatsize,
            SeatsizeKey,
            Platform,
            ClientOSName,
            Os_Group,
            IsAADPPAU,
            IsAADPPAU25,
            IsAADPPAU500,
            IsAADPPAU250,
            AADP1PAU,
            AADP2PAU,
            AADPPAU,
            //TPID
            COUNT(DISTINCT (IsAADPPAU == true? TPID : null )) AS AADPPaidAllAccounts,
            COUNT(DISTINCT (IsAADPPAU25 == true? TPID : null )) AS AADPPaid25AllAccounts,
            COUNT(DISTINCT (IsAADPPAU250 == true? TPID : null )) AS AADPPaid250AllAccounts,
            COUNT(DISTINCT (IsAADPPAU500 == true? TPID : null )) AS AADPPaid500AllAccounts,
            COUNT(DISTINCT (IsAADP1PAU == true? TPID : null )) AS AADP1PaidAllAccounts,
            COUNT(DISTINCT (IsAADP1PAU25 == true? TPID : null )) AS AADP1Paid25AllAccounts,
            COUNT(DISTINCT (IsAADP1PAU250 == true? TPID : null )) AS AADP1Paid250AllAccounts,
            COUNT(DISTINCT (IsAADP1PAU500 == true? TPID : null )) AS AADP1Paid500AllAccounts,
            COUNT(DISTINCT (IsAADP2PAU == true? TPID : null )) AS AADP2PaidAllAccounts,
            COUNT(DISTINCT (IsAADP2PAU25 == true? TPID : null )) AS AADP2Paid25AllAccounts,
            COUNT(DISTINCT (IsAADP2PAU250 == true? TPID : null )) AS AADP2Paid250AllAccounts,
            COUNT(DISTINCT (IsAADP2PAU500 == true? TPID : null )) AS AADP2Paid500AllAccounts,
            // TotalAccTPID
            COUNT(DISTINCT (IsAADPPAU == true? TotalAccTPID : null )) AS AADPPaidAccounts,
            COUNT(DISTINCT (IsAADPPAU25 == true? TotalAccTPID : null )) AS AADPPaid25Accounts,
            COUNT(DISTINCT (IsAADPPAU250 == true? TotalAccTPID : null )) AS AADPPaid250Accounts,
            COUNT(DISTINCT (IsAADPPAU500 == true? TotalAccTPID : null )) AS AADPPaid500Accounts,
            COUNT(DISTINCT (IsAADP1PAU == true? TotalAccTPID : null )) AS AADP1PaidAccounts,
            COUNT(DISTINCT (IsAADP1PAU25 == true? TotalAccTPID : null )) AS AADP1Paid25Accounts,
            COUNT(DISTINCT (IsAADP1PAU250 == true? TotalAccTPID : null )) AS AADP1Paid250Accounts,
            COUNT(DISTINCT (IsAADP1PAU500 == true? TotalAccTPID : null )) AS AADP1Paid500Accounts,
            COUNT(DISTINCT (IsAADP2PAU == true? TotalAccTPID : null )) AS AADP2PaidAccounts,
            COUNT(DISTINCT (IsAADP2PAU25 == true? TotalAccTPID : null )) AS AADP2Paid25Accounts,
            COUNT(DISTINCT (IsAADP2PAU250 == true? TotalAccTPID : null )) AS AADP2Paid250Accounts,
            COUNT(DISTINCT (IsAADP2PAU500 == true? TotalAccTPID : null )) AS AADP2Paid500Accounts,
            //OKTAF
            SUM(AADMAU) AS AADMAU,
            SUM(NativeAADMAU) AS NativeAADMAU,
            SUM(ADFSMAU) AS ADFSMAU,
            SUM(PingMAU) AS PingMAU,
            SUM(OktaMAU) AS OktaMAU,
            SUM(HDEMSMAU) AS HDEMSMAU,
            COUNT(DISTINCT (IsAADNewSource == true? TPID:null)) AS AADNewSourceAllAccounts,
            COUNT(DISTINCT (IsNativeAAD == true? TPID:null)) AS NativeAADAllAccounts,
            COUNT(DISTINCT (IsADFS == true? TPID:null)) AS ADFSAllAccounts,
            COUNT(DISTINCT (IsOkta == true? TPID:null)) AS OktaAllAccounts,
            COUNT(DISTINCT (IsPing == true? TPID:null)) AS PingAllAccounts,
            COUNT(DISTINCT (IsHDEMS == true? TPID:null)) AS HDEMSAllAccounts,
            COUNT(DISTINCT (IsAADNewSource == true? TotalAccTPID:null)) AS AADNewSourceAccounts,
            COUNT(DISTINCT (IsNativeAAD == true? TotalAccTPID:null)) AS NativeAADAccounts,
            IsAADNewSource,
            IsNativeAAD,
            IsADFS,
            IsOkta,
            IsPing,
            IsHDEMS,
            COUNT(DISTINCT (IsADFS == true? TotalAccTPID:null)) AS ADFSAccounts,
            COUNT(DISTINCT (IsOkta == true? TotalAccTPID:null)) AS OktaAccounts,
            COUNT(DISTINCT (IsPing == true? TotalAccTPID:null)) AS PingAccounts,
            COUNT(DISTINCT (IsHDEMS == true? TotalAccTPID:null)) AS HDEMSAccounts
        FROM Compete_AccAuthDetectionsEnr_FY24_Dtst
    );


// END BUSINESS LOGIC. Edit above this line

Compete_AccAuthDetectionsEnr_FY24_Dtst =
    SELECT AsOfDate,
           TPID,
           TPName,
           TotalAccTPID,
           IsOktaPipe,
           IsOktaPipe2,
           CustomerSegmentGroup,
           SubSegmentName,
           SegmentName,
           SegmentGroup,
           SegmentDerived,
           IDEAsSegment,
           IDEAsSegmentGroup,
           IndustryName,
           IndustrySummary,
           VerticalName,
           AreaName,
           RegionName,
           GeoLabel,
           IsMAL,
           IsActive,
           IsEDU,
           IsSMB,
           AAD_UniqueLogins,
           IsAADMAU1Plus,
           IsAADMAU500Plus,
           IsTPIDMAUQualified,
           IsTPIDA310,
           IsTPIDA210,
           IsTPIDA110,
           IsTPIDA010,
           IsTPIDOkta,
           IsS2500,
           IsS500,
           AADLoginMAU,
           IsAAD,
           IsAAD500,
           SaaSMAU,
           IsSaaS,
           AADIncentiveMAU,
           IsAADIncentive,
           AADPMAU,
           IsAADP,
           IsF500,
           IsF1000,
           IsG500,
           FortuneSector,
           Seatsize,
           Platform,
           ClientOSName,
           Os_Group,
           IsAADPPAU,
           IsAADPPAU25,
           IsAADPPAU500,
           IsAADPPAU250,
           IsAADP1PAU,
           IsAADP2PAU,
           IsAADP1PAU25,
           IsAADP2PAU25,
           IsAADP1PAU500,
           IsAADP2PAU500,
           IsAADP1PAU250,
           IsAADP2PAU250,
           AADP1PAU,
           AADP2PAU,
           AADPPAU,
           IsAADNewSource,
           IsNativeAAD,
           IsADFS,
           IsOkta,
           IsPing,
           IsHDEMS,
           AADMAU,
           NativeAADMAU,
           ADFSMAU,
           PingMAU,
           OktaMAU,
           HDEMSMAU,
           IsMSFTTenant
    FROM Compete_AccAuthDetectionsEnr_FY24_Dtst;

Compete_AccAuthDetectionsAgg_FY24_Dtst =
    SELECT AsOfDate,
           AsOfDateKey,
           SegmentKey,
           CustomerSegmentGroup,
           SubSegmentName,
           SegmentName,
           SegmentGroup,
           SegmentDerived,
           GeoKey,
           GeoLabel,
           AreaName,
           IndustryKey,
           IndustryName,
           VerticalName,
           TPIDAttributesKey,
           IsMAL,
           IsEDU,
           IsSMB,
           IsActive,
           IsS2500,
           IsS500,
           IsAADMAU1Plus,
           IsAADMAU500Plus,
           IsTPIDMAUQualified,
           IsTPIDA310,
           IsTPIDA210,
           IsTPIDA110,
           IsTPIDA010,
           IsTPIDOkta,
           TotalAccounts,
           AADMAU1PlusAccounts,
           AADMAU500PlusAccounts,
           QualifiedAccounts,
           A310Accounts,
           A210Accounts,
           A110Accounts,
           A010Accounts,
           OktaDetectedAccounts,
           SaasAccounts,
           AADPAccounts,
           AADAccounts,
           AADIncentiveAccounts,
           AADLoginMAU,
           IsAAD,
           IsAAD500,
           SaaSMAU,
           IsSaaS,
           AADIncentiveMAU,
           IsAADIncentive,
           AADPMAU,
           IsAADP,
           IsF500,
           IsF1000,
           IsG500,
           FortuneSector,
           Seatsize,
           SeatsizeKey,
           Platform,
           ClientOSName,
           Os_Group,
           IsAADPPAU,
           IsAADPPAU25,
           IsAADPPAU500,
           IsAADPPAU250,
           AADP1PAU,
           AADP2PAU,
           AADPPAU,
           IsAADNewSource,
           IsNativeAAD,
           IsADFS,
           IsOkta,
           IsPing,
           IsHDEMS,
           AADPPaidAllAccounts,
           AADPPaid25AllAccounts,
           AADPPaid250AllAccounts,
           AADPPaid500AllAccounts,
           AADP1PaidAllAccounts,
           AADP1Paid25AllAccounts,
           AADP1Paid250AllAccounts,
           AADP1Paid500AllAccounts,
           AADP2PaidAllAccounts,
           AADP2Paid25AllAccounts,
           AADP2Paid250AllAccounts,
           AADP2Paid500AllAccounts,
           AADPPaidAccounts,
           AADPPaid25Accounts,
           AADPPaid250Accounts,
           AADPPaid500Accounts,
           AADP1PaidAccounts,
           AADP1Paid25Accounts,
           AADP1Paid250Accounts,
           AADP1Paid500Accounts,
           AADP2PaidAccounts,
           AADP2Paid25Accounts,
           AADP2Paid250Accounts,
           AADP2Paid500Accounts,
           AADNewSourceAccounts,
           NativeAADAccounts,
           ADFSAccounts,
           OktaAccounts,
           PingAccounts,
           HDEMSAccounts,
           AADNewSourceAllAccounts,
           NativeAADAllAccounts,
           ADFSAllAccounts,
           OktaAllAccounts,
           PingAllAccounts,
           HDEMSAllAccounts,
           AADMAU,
           NativeAADMAU,
           ADFSMAU,
           PingMAU,
           OktaMAU,
           HDEMSMAU,
           OtherAttributesKey,
           IsOktaPipe,
           IsOktaPipe2,
           IDEAsSegment,
           IDEAsSegmentGroup,
           IndustrySummary,
           TotalAllAccounts,
           IsMSFTTenant
    FROM Compete_AccAuthDetectionsAgg_FY24_Dtst;


OUTPUT
    Compete_AccAuthDetectionsEnr_FY24_Dtst
TO SSTREAM
    @Compete_AccAuthDetectionsEnr_FY24_Dtst_outputPath
RANGE CLUSTERED BY
    TPID ASC
WITH STREAMEXPIRY
    @Compete_AccAuthDetectionsEnr_FY24_Dtst_streamExpiry;

OUTPUT
    Compete_AccAuthDetectionsAgg_FY24_Dtst
TO SSTREAM
    @Compete_AccAuthDetectionsAgg_FY24_Dtst_outputPath
RANGE CLUSTERED BY
    TPIDAttributesKey ASC
WITH STREAMEXPIRY
    @Compete_AccAuthDetectionsAgg_FY24_Dtst_streamExpiry;
