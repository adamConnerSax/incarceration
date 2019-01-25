-- This is here to satisfy the GHC stage restriction for the TH to get tableTypes
module DataSourcePaths where


veraDir = "./trends-data"
externalDataDir = "./external-data"
d4dRepoDir = "../incarceration-trends"
coloradoMoneyBailDir = d4dRepoDir ++ "/Colorado_ACLU/4-money-bail-analysis"

veraTrendsFP = veraDir ++ "/incarceration_trends.csv"


crimeStatsCO_FP = externalDataDir ++ "/ColoradoCrimeStatsFixed.csv"
countyDistrictCrosswalkCO_FP = externalDataDir ++ "/county-district-crosswalk.csv"

-- data added to this repo for now
countyBondCO_FP = externalDataDir ++ "/complete_county_bond_nospace.csv"
fipsByCountyFP = externalDataDir ++ "/CountyFIPS.csv"
censusSAIPE_FP = externalDataDir ++ "/medianHIByCounty.csv"
--countyBondCO_FP = externalDataDir ++ "/county_bond_data.csv"

