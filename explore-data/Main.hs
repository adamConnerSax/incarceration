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

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
--import           Control.Monad.Identity (Identity)
import qualified Data.List            as L
import qualified Data.Map             as M
import           Data.Maybe           (fromJust, fromMaybe, isJust)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec      as V
import qualified Dhall                as D
import           Frames               ((:.), (&:))
import qualified Frames               as F
import qualified Frames.CSV as F
import qualified Pipes                as P
import qualified Pipes.Prelude        as P

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

aggregateToMap :: Ord k => (a -> k) -> (a -> b -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key = getKey r
      newVal = combine r $ fromMaybe initial $ M.lookup key m
  in M.insert key newVal m -- we can probably do this more efficiently with alter or something.
 
aggregateFiltered :: Ord k => (c -> Maybe a) -> (a -> k) -> (a -> b -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateFiltered decode getKey combine initial m x = maybe m (aggregateToMap getKey combine initial m) $ decode x

mapToRecords :: Ord k => (k -> (F.Record ks)) -> M.Map k (F.Record cs) -> [F.Record (ks V.++ cs)]
mapToRecords keyToRec = fmap (\(k,dr) -> V.rappend (keyToRec k) dr) . M.toList 

pFilterMaybe :: Monad m => (a -> Maybe b) -> P.Pipe a b m ()
pFilterMaybe f =  P.map f P.>-> P.filter isJust P.>-> P.map fromJust  

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

-- NB: right now we are choosing to drop any rows which are missing the fields we use.
ratesByStateAndYear :: FL.Fold MaybeRow (F.FrameRec '[State, Year, CrimeRate, IncarcerationRate])
ratesByStateAndYear =
  let selectMaybe :: MaybeRow -> Maybe (F.Record '[Year, State, TotalPop, IndexCrime, TotalPrisonAdm]) = F.recMaybe . F.rcast
      getKey r = (r ^. state, r ^. year)
      getSubset :: F.Record '[Year, State, TotalPop, IndexCrime, TotalPrisonAdm] -> F.Record '[TotalPop, IndexCrime, TotalPrisonAdm]
      getSubset = F.rcast
      addSubset s soFar = ((soFar ^. totalPop) + (s ^. totalPop)) &: ((soFar ^. indexCrime) + (s ^. indexCrime)) &: ((soFar ^. totalPrisonAdm) + (s ^. totalPrisonAdm)) &: V.RNil
      emptyRow = 0 &: 0 &: 0 &: V.RNil
      keyToRecord (s,y) = s &: y &: V.RNil
  in FL.Fold (aggregateFiltered selectMaybe getKey (addSubset . getSubset) emptyRow) M.empty (F.toFrame . mapToRecords keyToRecord . fmap rates)
  
main :: IO ()
main = do
  config <- D.input D.auto "./config/explore-data.dhall"
  let trendsData = F.readTableMaybe $ trendsCsv config
  rBySY <- F.runSafeEffect $ FL.purely P.fold ratesByStateAndYear $ trendsData
  F.writeCSV "data/ratesByStateAndYear.csv" rBySY
