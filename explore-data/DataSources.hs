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

import qualified Frames          as F
import qualified Frames.Dsv      as F
import qualified Frames.InCore   as FI
import qualified Frames.ShowCSV  as F
-- we may need some of the inferred types
import           Data.Text       (Text)
import qualified Data.Text       as T
import qualified Data.Vector     as V

F.dsvTableTypes "IncarcerationTrends" veraTrendsFP
F.dsvTableTypes "SAIPE" censusSAIPE_FP
F.dsvTableTypes "FIPSByCounty" fipsByCountyFP
F.dsvTableTypes "CrimeStatsCO" crimeStatsCO_FP
F.dsvTableTypes "CountyDistrictCO" countyDistrictCrosswalkCO_FP
F.dsvTableTypes "CountyBondCO" countyBondCO_FP

type Row = IncarcerationTrends
type MaybeCols c = F.Rec (Maybe F.:. F.ElField) c
type MaybeRow r = MaybeCols (F.RecordColumns r)
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
