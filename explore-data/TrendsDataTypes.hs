{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell  #-}
{-# LANGUAGE TypeOperators    #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE QuasiQuotes           #-}
module TrendsDataTypes where

import qualified Frames    as F
import qualified Frames.ShowCSV          as F
import qualified Frames.InCore           as FI
-- we may need some of the inferred types
import qualified Data.Text as T
import           Data.Text (Text)
import qualified Data.Vector             as V

F.tableTypes "IncarcerationTrends" "trends-data/incarceration_trends.csv"
F.tableTypes "SAIPE" "external-data/medianHIByCounty.csv"
F.tableTypes "FIPSByCounty" "external-data/CountyFIPS.csv"
F.tableTypes "CrimeStatsCO" "../incarceration-trends/Colorado_ACLU/2-jail-pop-trends-analysis/colorado_crime_stats.csv"

incarcerationTrendsCsvPath = "trends-data/incarceration_trends.csv"
saipeCsvPath = "external-data/medianHIByCounty.csv"
fipsByCountyCsvPath = "external-data/CountyFIPS.csv"
crimeStatsCO_CsvPath = "../incarceration-trends/Colorado_ACLU/2-jail-pop-trends-analysis/colorado_crime_stats.csv"


type Row = IncarcerationTrends
type MaybeRow r = F.Rec (Maybe F.:. F.ElField) (F.RecordColumns r)
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
