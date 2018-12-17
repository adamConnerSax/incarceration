{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell  #-}
module TrendsDataTypes where

import qualified Frames    as F
-- we may need some of the inferred types
import           Data.Text (Text)

F.tableTypes "IncarcerationTrends" "trends-data/incarceration_trends.csv"

