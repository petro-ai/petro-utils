# ----------------- MONTHLY PRODUCTION ------------------
class MonthlyProduction:
    key_cols = [
        'wellId',
        'prodDate',
    ]
    pai_cols = [
        'wellId',
        'prodDate',
        'uptime_days',
        'duration_days',
        'gasLiftRate_McfPerDay',
        'gasLiftVol_Mcf',
        'oilRate_bblPerDay',
        'oilVol_bbl',
        'oilCum_bbl',
        'condensateRate_bblPerDay',
        'condensateVol_bbl',
        'condensateCum_bbl',
        'gasRate_McfPerDay',
        'gasVol_Mcf',
        'gasCum_Mcf',
        'waterRate_bblPerDay',
        'waterVol_bbl',
        'waterCum_bbl',
        'boeRate_boePerDay',
        'boeVol_boe',
        'boeCum_boe',
        'waterInjRate_bblPerDay',
        'waterInjVol_bbl',
        'waterInjCum_bbl',
        'gasInjRate_McfPerDay',
        'gasInjVol_Mcf',
        'gasInjCum_Mcf',
        'wellName'
    ]
    mapper = {
        'WellID': 'wellId',
        'GasRate_McfPerDay': 'gasRate_McfPerDay',
        'ProducingMonth': 'prodDate',
        'ProducingDays': 'uptime_days',
        'CDLiquids_BBLPerDAY': 'oilRate_bblPerDay',
        'LiquidsProd_BBL': 'oilVol_bbl',
        'CumLiquids_BBL': 'oilCum_bbl',
        'CDGas_MCFPerDAY': 'gasRate_McfPerDay',
        'GasProd_MCF': 'gasVol_Mcf',
        'CumGas_MCF': 'gasCum_Mcf',
        'CDWater_BBLPerDAY': 'waterRate_bblPerDay',
        'WaterProd_BBL': 'waterVol_bbl',
        'CumWater_BBL': 'waterCum_bbl',
        'CDProd_BOEPerDAY': 'boeRate_boePerDay',
        'Prod_BOE': 'boeVol_boe',
        'CumProd_BOE': 'boeCum_boe',
        'CalendarDayInjectionWater_BBLPerDAY': 'waterInjRate_bblPerDay',
        'InjectionWater_BBL': 'waterInjVol_bbl',
        'CalendarDayInjectionGas_MCFPerDAY': 'gasInjRate_McfPerDay',
        'InjectionGas_MCF': 'gasInjVol_Mcf',
        'WellName': 'wellName'
    }
    relevant_columns = [
        'wellId',
        'prodDate',
        'oilRate_bblPerDay',
        'oilVol_bbl',
        'gasRate_McfPerDay',
        'gasVol_Mcf',
        'waterRate_bblPerDay',
        'waterVol_bbl',
        'oilCum_bbl', 
        'gasCum_Mcf',
        'waterCum_bbl'
    ]
    
# --------------------- WELL ----------------------------
class Well:
    key_cols = [
        'wellId',
    ]
    pai_cols = [
        'abandonmentDate',
        'api10',
        'api12',
        'api14',
        'basinName',
        'bottomHoleLoc_lat',
        'bottomHoleLoc_lon',
        'completionDate',
        'countryName',
        'countyName',
        'drillAbandonmentDate',
        'elevationGround_ft',
        'elevationKB_ft',
        'entityType',
        'fieldName',
        'finalDrillingDate',
        'firstProductionDate',
        'formationName',
        'fracStages',
        'heelMd_ft',
        'heelTvdss_ft',
        'lastProductionDate',
        'lateralLength_ft',
        'leaseName',
        'leaseNumber',
        'measuredDepth_ft',
        'midPointLoc_lat',
        'midPointLoc_lon',
        'name',
        'netRevenueInterest',
        'operatorName',
        'perforationLower_ft',
        'perforationUpper_ft',
        'permitDate',
        'primaryProduct',
        'reserveCategory',
        'reservoirName',
        'rigId',
        'rigReleaseDate',
        'spudDate',
        'stateName',
        'statusCurrent',
        'statusCurrentDate',
        'surfaceLoc_lat',
        'surfaceLoc_lon',
        'suspendedDate',
        'toeMd_ft',
        'totalDepth_ft',
        'totalFluidPumped_bbl',
        'totalProppant_lb',
        'tvd_ft',
        'tvdss_ft',
        'wellId',
        'wellNumber',
        'workingInterest'
    ]
    mapper = {
        'Api12': 'api12',
        'Api14' : 'api14',
        'API_UWI_14': 'api14',
        'WellID': 'wellId',
        'WellName': 'name',
        'Country': 'countryName',
        'StateProvince': 'stateName',
        'County': 'countyName',
        'Lease': 'leaseNumber',
        'LeaseName': 'leaseName',
        'Formation': 'formationName',
        'FirstProdDate': 'firstProductionDate',
        'Latitude': 'surfaceLoc_lat',
        'Longitude': 'surfaceLoc_lon',
        'Latitude_BH': 'bottomHoleLoc_lat',
        'Longitude_BH': 'bottomHoleLoc_lon',
        'TVD_FT': 'tvd_ft',
        'MD_FT': 'measuredDepth_ft',
        'Field': 'fieldName',
        'ENVOperator': 'operatorName',
        'ENVBasin': 'basinName',
        'SpudDate': 'spudDate',
        'RigReleaseDate': 'rigReleaseDate',
        'CompletionDate': 'completionDate',
        'LateralLength_FT': 'lateralLength_ft',
        'UpperPerf_FT': 'perforationUpper_ft', 
        'TotalWaterPumped_GAL': 'totalFluidPumped_bbl',
        'Proppant_LBS': 'totalProppant_lb',
        'TotalProppant_lb': 'totalProppant_lb',
        'LastProducingMonth': 'lastProductionDate',
        'ENVWellStatus': 'statusCurrent',
        'ENVWellboreStatus': 'statusCurrentDate',
        'TotalFluidPumped_BBL': 'totalFluidPumped_bbl',
        'WellNumber':'wellNumber',
        'ElevationKB_FT': 'elevationKB_ft',
        'ElevationGL_FT' : 'elevationGround_ft',
        'LowerPerf_FT': 'perforationLower_ft',
        'DrillingEndDate':'finalDrillingDate',
        'PermitApprovedDate' : 'permitDate'
    }
    relevant_columns = [
        'completionDate',
        'operatorName',
        'lateralLength_ft', 
        'api10', 
        'api12',
        'api14',
        'name',
        'entityType',
        'primaryProduct',
        'statusCurrent',
        'firstProductionDate',
        'leaseName',
        'wellNumber',
        'fieldName',
        'lateralLength_ft',
        'fracStages',
        'totalProppant_lb',
        'totalFluidPumped_bbl',
        'formationName',
        'stateName',
        'countyName',
        'surfaceLoc_lat',
        'surfaceLoc_lon',
        'bottomHoleLoc_lat',
        'bottomHoleLoc_lon'
    ]

#------------------ SURVEY --------------------
class WellDirectionalSurveyPoint:
    key_cols = [
        'wellId',
        'md_ft',
    ]
    pai_cols = [
        "wellId",
        "md_ft",
        "tvd_ft",
        "tvdss_ft",
        "xOffset_ft",
        "yOffset_ft",
        "azimuth_deg",
        "inclination_deg",
        "latitude",
        "longitude",
        "wellName"
    ]
    mapper = {
        'TVD_FT' : 'tvd_ft',
        'SubseaElevation_FT' : 'tvdss_ft',
        'MeasuredDepth_FT': 'md_ft',
        'Azimuth_DEG': 'azimuth_deg',
        'Inclination_DEG' : 'inclination_deg',
        'API_UWI':'wellId',
        'N_S':'yOffset_ft',
        'E_W':'xOffset_ft',
        'Latitude':'latitude',
        'Longitude' : 'longitude'
    }
    relevant_columns = [
        'wellId', 
        'md_ft', 
        'tvd_ft', 
        'tvdss_ft', 
        'latitude', 
        'longitude'
    ]

#---------------WELL LOOKUP--------------------------------
class WellLookup:
    key_cols = [
        'wellId',
    ]
    pai_cols =[
        'wellId',
        'prodWellId',
        'surveyWellId'
    ]
    mapper = {
        'wellID':'wellId',
        'prodWellID':'prodWellId',
        'surveyWellID': 'surveyWellId'
    }
    relevant_columns = [
        'wellId',
        'prodWellId',
        'surveyWellId'
    ]

#----------------INVENTORY WELLS-------------------------
class InventoryWells:
    key_cols = [
        'clientWellId',
        'scenarioName',
    ]
    pai_cols = [
        'clientWellId',
        'dsuId',
        'scenarioName',
        'wellName',
        'ftp_lat',
        'ftp_lon',
        'ltp_lat',
        'ltp_lon',
        'shl_lat',
        'shl_lon',
        'lateralLength_ft',
        'interval',
        'relativeLanding',
        'completionDate'
    ]
    mapper = {
        'API_TXT'  : 'clientWellId',
        'GIS_ID':'dsuId',
        'Enersight_Name': 'scenarioName',
        'Well_Name':'wellName',
        'FTP_LAT':'ftp_lat',
        'FTP_LONG' :'ftp_lon',
        'LTP_LAT':'ltp_lat',
        'LTP_LONG':'ltp_lon',
        'SHL_LAT':'shl_lat',
        'SHL_LONG':'shl_lon',
        'bhl_lat': 'ltp_lat',
        'bhl_lon': 'ltp_lon',
        'Plan_Completed_Lateral_Length': 'lateralLength_ft',
    }

#----------------GRID AND STRUCTURE-------------------------
class GridStructureData:
    key_cols = [
       'interval',
       'x',
       'y', 
    ]
    pai_cols = [
        'interval',
        'x',
        'y',
        'tvdss_ft',
    ]
    mapper = {
        'Interval': 'interval',
        'X': 'x',
        'Y': 'y',
        'TVDSS_FT' : 'tvdss_ft',
        'SubseaElevation_FT' : 'tvdss_ft',
    }
    relevant_columns = [
        'interval',
        'x',
        'y',
        'tvdss_ft',
    ]

class GridAttributeData:
    key_cols = [
        'name',
        'x',
        'y',
    ]
    pai_cols = [
        'name',
        'x',
        'y',
        'value',
    ]
    mapper = {
        'Name': 'name',
        'X': 'x',
        'Y': 'y',
        'Value': 'value',
        'attributeValue': 'value',
    }
    relevant_columns = [
        'name',
        'x',
        'y',
        'value',
    ]

class GridStructureHeader:
    key_cols = [
        'interval',
    ]
    pai_cols = [
        'interval',
        'sourceCRS_proj4',
    ]
    mapper = {
        'Interval': 'interval',
        'SourceCRS_proj4': 'sourceCRS_proj4',
    }
    relevant_columns = [
        'interval',
        'sourceCRS_proj4',
    ]

class GridAttributeHeader:
    key_cols = [
        'name',   
    ]
    pai_cols = [
        'name',
        'attributeName',
        'sourceCRS_proj4',
        'uom',
        'unit',
        'interval',
    ]
    mapper = {
        'Name': 'name',
        'AttributeName': 'attributeName',
        'sourceCRS_proj4': 'SourceCRS_Proj4',
        'UOM': 'uom',
        'Unit': 'unit',
        'Interval': 'interval',
    }
    relevant_columns = [
        'name',
        'attributeName',
        'sourceCRS_proj4',
        'uom',
        'unit',
        'interval',
    ]





