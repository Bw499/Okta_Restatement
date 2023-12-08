MODULE "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsTenantProfile/Resources/v4/IDEAsTenantProfileExtension_v2.module" AS IDEAsTenantProfileExtensionModule;

#DECLARE TPIDDate DateTime = DateTime.Parse("2023-11-29");
#DECLARE windowStart DateTime = DateTime.Parse("2022-11-30");
#DECLARE FederationCounts_windowStart DateTime = new DateTimeOffset(@windowStart).UtcDateTime;
#DECLARE IDEAsTenantProfileExtension_windowStart DateTime = new DateTimeOffset(@windowStart).UtcDateTime;



// Get TenantId and TPID from GoLive
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

TenantTPId =
    SELECT TenantId.ToUpper() AS TenantId,
           MSSalesTopParentOrgId AS TPID,
           MIN(CustomerSegmentGroup) AS CustomerSegmentGroup
    FROM IDEAsTenantsProfile;


// SegmentDerived
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
            HistoryDate = @TPIDDate
        )
    );





// Get TPID Flag info
FortuneFlagsProfile =
    SELECT TPID,
           IsF1000,
           IsF500,
           IsG500
    FROM
    (
        VIEW "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsFortuneFlagsProfile/Views/v1/IDEAsFortuneFlagsProfile.view"
    );

// FederationCounts -- Okta Federation, Ping Federation, etc. by TPID
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

FederationCounts =  SELECT *.Except(ContextId), ContextId.ToString().ToUpper() AS TenantId
                    FROM FederationCounts;

FederationCounts =
    SELECT TPID,
           IF(AuthType != "Federated", AuthType, StsProduct) AS AuthProduct,
           SUM(IF(UniqueActiveUserCount IS NULL, 0, UniqueActiveUserCount)) AS AADMAU
    FROM FederationCounts AS a
         RIGHT OUTER JOIN
             TenantTPId
         ON a.TenantId == TenantTPId.TenantId
    HAVING AADMAU > 0;


TenantTPId =
    SELECT TenantTPId.TPID, 
           IF(TPID.IsEDU == true, "EDU",
           IF(TPID.SegmentGroup == "Enterprise", "Enterprise",
           IF(TPID.SegmentName != NULL AND TPID.SegmentName.ToLower().Contains("scale"), "SMC Scale",
           IF(SegmentGroup == "SMC Corporate", "SMC Managed",
           IF(TenantTPId.CustomerSegmentGroup == "SMC - SMB", "SMB", "Others"))))) AS SegmentDerived,
           AreaName,
           IF(TPName != NULL, TPName.Replace(",", ""), NULL) AS TPName
    FROM TenantTPId
    LEFT OUTER JOIN
        TPIDAttributes AS TPID
        ON TenantTPId.TPID == TPID.TPID;

TenantTPId =
    SELECT *
    FROM TenantTPId
    WHERE SegmentDerived IN("Enterprise", "SMC Managed");

FederationCounts =
     SELECT TPID,
            SUM(AADMAU) AS AADMAU,
            SUM(IF (AuthProduct IN ("PHS","PTA","Cloud Only" ), AADMAU, 0)) AS NativeAADMAU,
            SUM(IF (AuthProduct == "ADFS",AADMAU, 0)) AS ADFSMAU,
            SUM(IF (AuthProduct == "Ping",AADMAU, 0)) AS PingMAU,
            SUM(IF (AuthProduct == "Okta",AADMAU, 0)) AS OktaMAU,
            SUM(IF (AuthProduct == "HDEMS",AADMAU, 0)) AS HDEMSMAU
     FROM FederationCounts;

FederationCounts =
     SELECT TPID,
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


// Get AADP data
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


// End of Business Logic


output =
    SELECT TenantTPId.TPID,
           TenantTPId.SegmentDerived,
           AreaName,
           TPName,
           IsF1000,
           IsF500,
           IsG500,
           AADPPAU,
           IsAADPPAU,
           IsAADPPAU25,
           IsAADPPAU250,
           IsAADPPAU500,
           IF(F.IsNativeAAD == true, true, false) AS IsNativeAAD,
           IF(F.IsADFS == true, true, false) AS IsADFS,
           IF(F.IsOkta == true, true, false) AS IsOkta,
           IF(F.IsPing == true, true, false) AS IsPing,
           IF(F.IsHDEMS == true, true, false) AS IsHDEMS,
           SUM(F.AADMAU) AS AADMAU,
           SUM(F.NativeAADMAU) AS NativeAADMAU,
           SUM(F.ADFSMAU) AS ADFSMAU,
           SUM(F.PingMAU) AS PingMAU,
           SUM(F.OktaMAU) AS OktaMAU,
           SUM(F.HDEMSMAU) AS HDEMSMAU
    FROM TenantTPId
    LEFT OUTER JOIN
        FortuneFlagsProfile
        ON TenantTPId.TPID == FortuneFlagsProfile.TPID
    LEFT OUTER JOIN
        PAU_AADP_FLAGS
        ON TenantTPId.TPID == PAU_AADP_FLAGS.TPID
    LEFT OUTER JOIN
        FederationCounts AS F
        ON TenantTPId.TPID == F.TPID;



#DECLARE OutputStream1 string = string.Format("/local/users/beca/Okta/Restatement_{0}_{1}_{2}.ss", @windowStart.Year.ToString(), @windowStart.Month.ToString("D2"), @windowStart.Day.ToString("D2"));
OUTPUT output
TO SSTREAM @OutputStream1; 
