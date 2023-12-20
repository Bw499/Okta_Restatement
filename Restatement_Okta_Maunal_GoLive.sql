MODULE "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsTenantProfile/Resources/v4/IDEAsTenantProfileExtension_v2.module" AS IDEAsTenantProfileExtensionModule;

#DECLARE TPIDDate DateTime = DateTime.Parse("2023-10-31"); 

#IF ("@@DaysToSubtract@@".StartsWith("@@"))
    #DECLARE DaysToSubtract int = -27; // -27 for RL28, -6 for RL7, -0 for Daily 
#ELSE
    #DECLARE DaysToSubtract int = @@DaysToSubtract@@; // -27 for RL28, -6 for RL7, -0 for Daily 
#ENDIF
#IF ("@@SliceStartTime@@".StartsWith("@@"))
    #DECLARE SliceStartTime string = "2022-10-31";
#ELSE
    #DECLARE SliceStartTime string = "@@SliceStartTime@@";
#ENDIF

#DECLARE windowStart DateTime = DateTimeOffset.Parse(@SliceStartTime).DateTime;
#DECLARE windowEnd DateTime = DateTimeOffset.Parse(@SliceStartTime).DateTime;

#DECLARE FederationCounts_windowStart DateTime = new DateTimeOffset(@windowStart).UtcDateTime;
#DECLARE AADAuthFed_windowStart DateTime = new DateTimeOffset(@windowStart).UtcDateTime;
#DECLARE AADAuthFed_windowEnd DateTime = new DateTimeOffset(@windowEnd).Add(TimeSpan.Parse("-00:01:00")).UtcDateTime;
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
           CustomerSegmentGroup
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


#IF(new DateTimeOffset(@windowStart) >= DateTimeOffset.Parse("2023-09-07T00:00:00Z").UtcDateTime)
    FederationCounts =  SELECT *.Except(TenantId), TenantId.ToUpper() AS TenantId
                    FROM AADAuthFed;
#ELSE
    FederationCounts =  SELECT *.Except(ContextId), ContextId.ToString().ToUpper() AS TenantId
                    FROM FederationCounts;
#ENDIF



FederationCounts =
    SELECT TPID,
           IF(AuthType != "Federated", AuthType, StsProduct) AS AuthProduct,
           SUM(IF(UniqueActiveUserCount IS NULL, 0, UniqueActiveUserCount)) AS AADMAU
    FROM FederationCounts AS a
         LEFT OUTER JOIN
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
    FROM (SELECT TPID, MIN(CustomerSegmentGroup) AS CustomerSegmentGroup FROM TenantTPId) AS TenantTPId
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



#DECLARE OutputStream1 string = string.Format("/local/users/beca/Okta/TPID_Restatement_{0}_{1}_{2}.ss", @windowStart.Year.ToString(), @windowStart.Month.ToString("D2"), @windowStart.Day.ToString("D2"));
OUTPUT output
TO SSTREAM @OutputStream1; 



output_agg_WW =
    SELECT COUNT (DISTINCT TPID) AS TotalAllAccounts,
           //COUNT(DISTINCT (IsTPIDOkta == true? TPID : null)) AS TPIDOktaAllAccounts,
           COUNT(DISTINCT (IsAADPPAU == true? TPID : null)) AS AADPPaidAllAccounts,
           COUNT(DISTINCT (IsAADPPAU25 == true? TPID : null)) AS AADPPaid25AllAccounts,
           COUNT(DISTINCT (IsAADPPAU250 == true? TPID : null)) AS AADPPaid250AllAccounts,
           COUNT(DISTINCT (IsAADPPAU500 == true? TPID : null)) AS AADPPaid500AllAccounts,
           COUNT(DISTINCT (IsNativeAAD == true? TPID : null)) AS NativeAADAllAccounts,
           COUNT(DISTINCT (IsADFS == true? TPID : null)) AS ADFSAllAccounts,
           COUNT(DISTINCT (IsOkta == true? TPID : null)) AS OktaAllAccounts,
           COUNT(DISTINCT (IsPing == true? TPID : null)) AS PingAllAccounts
    FROM output;
             
             
//#DECLARE OutputStream2 string = string.Format("/local/users/beca/Okta/agg_managed_Okta_{0}_{1}_{2}.ss", @windowStart.Year.ToString(), @windowStart.Month.ToString("D2"), @windowStart.Day.ToString("D2"));
//OUTPUT output_agg
//TO SSTREAM @OutputStream2;




output_agg_US =
    SELECT COUNT(DISTINCT TPID) AS TotalAllAccounts,
           //COUNT(DISTINCT (IsTPIDOkta == true? TPID : null)) AS US_TPIDOktaAllAccounts,
           COUNT(DISTINCT (IsAADPPAU == true? TPID : null)) AS US_AADPPaidAllAccounts,
           COUNT(DISTINCT (IsAADPPAU25 == true? TPID : null)) AS US_AADPPaid25AllAccounts,
           COUNT(DISTINCT (IsAADPPAU250 == true? TPID : null)) AS US_AADPPaid250AllAccounts,
           COUNT(DISTINCT (IsAADPPAU500 == true? TPID : null)) AS US_AADPPaid500AllAccounts,
           COUNT(DISTINCT (IsNativeAAD == true? TPID : null)) AS US_NativeAADAllAccounts,
           COUNT(DISTINCT (IsADFS == true? TPID : null)) AS US_ADFSAllAccounts,
           COUNT(DISTINCT (IsOkta == true? TPID : null)) AS US_OktaAllAccounts,
           COUNT(DISTINCT (IsPing == true? TPID : null)) AS US_PingAllAccounts
    FROM output
    WHERE AreaName == "United States";
             
             
//#DECLARE OutputStream3 string = string.Format("/local/users/beca/Okta/agg_US_managed_Okta_{0}_{1}_{2}.ss", @windowStart.Year.ToString(), @windowStart.Month.ToString("D2"), @windowStart.Day.ToString("D2"));
//OUTPUT output_agg_US
//TO SSTREAM @OutputStream3;



output_agg_F500 =
    SELECT COUNT(DISTINCT TPID) AS TotalAllAccounts,
           //COUNT(DISTINCT (IsTPIDOkta == true? TPID : null)) AS F500_TPIDOktaAllAccounts,
           COUNT(DISTINCT (IsAADPPAU == true? TPID : null)) AS F500_AADPPaidAllAccounts,
           COUNT(DISTINCT (IsAADPPAU25 == true? TPID : null)) AS F500_AADPPaid25AllAccounts,
           COUNT(DISTINCT (IsAADPPAU250 == true? TPID : null)) AS F500_AADPPaid250AllAccounts,
           COUNT(DISTINCT (IsAADPPAU500 == true? TPID : null)) AS F500_AADPPaid500AllAccounts,
           COUNT(DISTINCT (IsNativeAAD == true? TPID : null)) AS F500_NativeAADAllAccounts,
           COUNT(DISTINCT (IsADFS == true? TPID : null)) AS F500_ADFSAllAccounts,
           COUNT(DISTINCT (IsOkta == true? TPID : null)) AS F500_OktaAllAccounts,
           COUNT(DISTINCT (IsPing == true? TPID : null)) AS F500_PingAllAccounts
    FROM output
    WHERE IsF500 == true;


//#DECLARE OutputStream4 string = string.Format("/local/users/beca/Okta/agg_F500_managed_Okta_{0}_{1}_{2}.ss", @windowStart.Year.ToString(), @windowStart.Month.ToString("D2"), @windowStart.Day.ToString("D2"));
//OUTPUT output_agg_F500
//TO SSTREAM @OutputStream4;


output_agg =
    SELECT *
    FROM
    (
    SELECT *
    FROM output_agg_WW
    UNION ALL
    SELECT *
    FROM output_agg_US
UNION ALL
SELECT * FROM output_agg_F500
    );
                               
#DECLARE OutputStream2 string = string.Format("/local/users/beca/Okta/agg_managed_Okta_{0}_{1}_{2}.ss", @windowStart.Year.ToString(), @windowStart.Month.ToString("D2"), @windowStart.Day.ToString("D2"));
OUTPUT output_agg
TO SSTREAM @OutputStream2;

