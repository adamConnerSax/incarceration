{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
module Main where

import           TrendsDataTypes

import qualified Control.Foldl      as FL
import           Control.Lens       ((^.))
import qualified Control.Lens       as L
--import           Control.Monad.Identity (Identity)
import qualified Data.List          as L
import qualified Data.Map           as M
import           Data.Maybe         (fromJust, isJust)
import           Data.Text          (Text)
import qualified Data.Text          as T
import qualified Data.Vinyl         as V
import qualified Data.Vinyl.Functor as V
import qualified Data.Vinyl.XRec    as V
import qualified Dhall              as D
import           Frames             ((:.), (&:))
import qualified Frames             as F
import qualified Pipes              as P
import qualified Pipes.Prelude      as P

data Config = Config
  {
    trendsCsv :: FilePath
  , fipsCode  :: Integer
  } deriving (D.Generic)

instance D.Interpret Config



type Row = IncarcerationTrends
type MaybeRow = F.Rec (Maybe F.:. F.ElField) (F.RecordColumns Row)

F.declareColumn "IncarcerationRate" ''Double
F.declareColumn "CrimeRate" ''Double

maybeTest :: (a -> Bool) -> Maybe a -> Bool
maybeTest t = maybe False t

fipsFilter x = maybeTest (== x) . V.toHKD . F.rget @Fips -- use F.rgetField?
stateFilter s = maybeTest (== s) . V.toHKD . F.rget @State
yearFilter y = maybeTest (== y) . V.toHKD . F.rget @Year

rates :: F.Record [TotalPop, IndexCrime, TotalPrisonAdm] -> F.Record [CrimeRate, IncarcerationRate]
rates r = ((fromIntegral $ r ^. indexCrime) / (fromIntegral $ r ^. totalPop)) &: ((fromIntegral $ r ^. totalPrisonAdm) / (fromIntegral $ r ^. indexCrime)) &: V.RNil

foldYear :: F.MonadSafe m => Int -> P.Producer (F.Record '[Year, TotalPop, IndexCrime, TotalPrisonAdm]) m () -> m (F.Record [TotalPop, IndexCrime, TotalPrisonAdm])
foldYear y frame =
  let totalF l = FL.Fold (\p r -> p + (r ^. l)) 0 id
      toRec = ((\p ic pa -> p &: ic &: pa &: V.RNil) <$> totalF totalPop <*> totalF indexCrime <*> totalF totalPrisonAdm)
  in FL.purely P.fold toRec $ frame P.>-> P.filter ((== y) . L.view year)

-- Get data for a given county over time.  LEARNING!

main :: IO ()
main = do
  config <- D.input D.auto "./config/explore-data.dhall"
  let trendsData :: F.MonadSafe m => P.Producer MaybeRow m ()
      trendsData = (F.readTableMaybe $ trendsCsv config) P.>-> P.filter (stateFilter "NY")
      selectMaybe :: MaybeRow -> Maybe (F.Record '[Year, TotalPop, IndexCrime, TotalPrisonAdm])
      selectMaybe = F.recMaybe . F.rcast
  res <- fmap rates . F.runSafeEffect $ foldYear 1989 $ (trendsData P.>-> P.map selectMaybe P.>-> P.filter isJust P.>-> P.map fromJust)
  print res
