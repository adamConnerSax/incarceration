{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module DataSources
  (
    module DataSourcePaths
  , module DataSources
  )
where

-- NB: required for TH stage restriction
import           DataSourcePaths

import           Frames.MaybeUtils (MaybeRow)

import qualified Frames            as F
--import qualified Frames.CSV        as F
import qualified Frames.InCore     as FI
import qualified Frames.ShowCSV    as F
-- we may need some of the inferred types
import           Data.Text         (Text)
import qualified Data.Text         as T
import qualified Data.Vector       as V

F.tableTypes "IncarcerationTrends" veraTrendsFP
F.tableTypes "SAIPE" censusSAIPE_FP
F.tableTypes "FIPSByCounty" fipsByCountyFP
F.tableTypes "CrimeStatsCO" crimeStatsCO_FP
F.tableTypes "CountyDistrictCO" countyDistrictCrosswalkCO_FP
F.tableTypes "CountyBondCO" countyBondCO_FP

--type Row = IncarcerationTrends
type MaybeITrends = MaybeRow IncarcerationTrends

data GenderT = Male | Female deriving (Show,Enum,Bounded,Ord, Eq) -- we ought to have NonBinary here as well, but the data doesn't.
type instance FI.VectorFor GenderT = V.Vector
instance F.ShowCSV GenderT where
  showCSV = T.pack . show

F.declareColumn "ImprisonedPerCrimeRate" ''Double
F.declareColumn "CrimeRate" ''Double
F.declareColumn "IncarcerationRate" ''Double
F.declareColumn "PrisonAdmRate" '' Double
F.declareColumn "Gender" ''GenderT

proxyYear = [F.pr1|Year|]
