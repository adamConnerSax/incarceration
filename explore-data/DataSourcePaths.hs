-- This is here to satisfy the GHC stage restriction for the TH to get tableTypes
module DataSourcePaths where

--- VERA
veraDir = "./trends-data"
veraTrendsFP = veraDir ++ "/incarceration_trends.csv"

-- D4D Repo
d4dRepoDir = "../incarceration-trends"
crimeStatsCO_FP = d4dRepoDir ++ "/Colorado_ACLU/2-jail-pop-trends-analysis/colorado_crime_stats.csv"
countyBondCO_FP = d4dRepoDir ++ "/Colorado_ACLU/4-money-bail-analysis/county_bond_data.csv"
countyDistrictCrosswalkCO_FP = d4dRepoDir ++ "/Colorado_ACLU/4-money-bail-analysis/county-district-crosswalk.csv"

-- data added to this repo for now
externalDataDir = "./external-data"
fipsByCountyFP = externalDataDir ++ "/CountyFIPS.csv"
censusSAIPE_FP = externalDataDir ++ "/medianHIByCounty.csv"
--countyBondCO_FP = externalDataDir ++ "/county_bond_data.csv"

