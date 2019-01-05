{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell  #-}
module TrendsDataTypes where

import qualified Frames    as F
-- we may need some of the inferred types
import           Data.Text (Text)

F.tableTypes "IncarcerationTrends" "trends-data/incarceration_trends.csv"

F.tableTypes "SAIPE" "external-data/medianHIByCounty.csv"

F.tableTypes "FIPSByCounty" "../incarceration-trends/US_trends/CountyFIPS.csv"
