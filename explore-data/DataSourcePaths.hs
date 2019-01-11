-- This is here to satisfy the GHC stage restriction for the TH to get tableTypes
module DataSourcePaths where


veraDir = "./trends-data"
externalDataDir = "./external-data"
d4dRepoDir = "../incarceration-trends"

veraTrendsFP = veraDir ++ "/incarceration_trends.csv"


crimeStatsCO_FP = d4dRepoDir ++ "/Colorado_ACLU/2-jail-pop-trends-analysis/colorado_crime_stats.csv"
--countyBondCO_FP = d4dRepoDir ++ "/Colorado_ACLU/4-money-bail-analysis/county_bond_data.csv"
--countyBondCO_FP = d4dRepoDir ++ "/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"
countyDistrictCrosswalkCO_FP = d4dRepoDir ++ "/Colorado_ACLU/4-money-bail-analysis/county-district-crosswalk.csv"

-- data added to this repo for now
countyBondCO_FP = externalDataDir ++ "/complete_county_bond_nospace.csv"
fipsByCountyFP = externalDataDir ++ "/CountyFIPS.csv"
censusSAIPE_FP = externalDataDir ++ "/medianHIByCounty.csv"
--countyBondCO_FP = externalDataDir ++ "/county_bond_data.csv"

