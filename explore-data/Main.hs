{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE QuasiQuotes #-}
module Main where

import           TrendsDataTypes

import qualified Language.R.Instance as R
import Language.R.QQ

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import qualified Data.List            as L
import qualified Data.Map             as M
import           Data.Maybe           (fromJust, fromMaybe, isJust)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec      as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Dhall                as D
import           Frames               ((:.), (&:))
import qualified Frames               as F
import qualified Frames.CSV           as F
import qualified Frames.InCore        as F
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import Control.Arrow (second)
import Data.Proxy (Proxy(..))
import qualified Data.Foldable as Fold

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

-- Utils.  Should get their own lib
aggregateToMap :: Ord k => (a -> k) -> (a -> b -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key = getKey r
      newVal = Just . combine r . fromMaybe initial 
  in M.alter newVal key m --M.insert key newVal m 
 
aggregateFiltered :: Ord k => (c -> Maybe a) -> (a -> k) -> (a -> b -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateFiltered decode getKey combine initial m x = maybe m (aggregateToMap getKey combine initial m) $ decode x
--


rates :: F.Record [TotalPop, IndexCrime, TotalPrisonAdm] -> F.Record [CrimeRate, IncarcerationRate]
rates r = ((fromIntegral $ r ^. indexCrime) / (fromIntegral $ r ^. totalPop)) &: ((fromIntegral $ r ^. totalPrisonAdm) / (fromIntegral $ r ^. indexCrime)) &: V.RNil

-- NB: right now we are choosing to drop any rows which are missing the fields we use.
ratesByStateAndYear :: FL.Fold MaybeRow (F.FrameRec '[Year, State, CrimeRate, IncarcerationRate])
ratesByStateAndYear =
  let selectMaybe :: MaybeRow -> Maybe (F.Record '[Year, State, TotalPop, IndexCrime, TotalPrisonAdm]) = F.recMaybe . F.rcast
      getKey :: F.Record '[Year, State, TotalPop, IndexCrime, TotalPrisonAdm] -> F.Record '[Year, State] = F.rcast --r = (r ^. state, r ^. year)
      getSubset :: F.Record '[Year, State, TotalPop, IndexCrime, TotalPrisonAdm] -> F.Record '[TotalPop, IndexCrime, TotalPrisonAdm]
      getSubset = F.rcast
      addSubset s soFar = ((soFar ^. totalPop) + (s ^. totalPop)) &: ((soFar ^. indexCrime) + (s ^. indexCrime)) &: ((soFar ^. totalPrisonAdm) + (s ^. totalPrisonAdm)) &: V.RNil
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in FL.Fold (aggregateFiltered selectMaybe getKey (addSubset . getSubset) emptyRow) M.empty (F.toFrame . fmap (uncurry V.rappend . second rates) . M.toList) --mapToRecords id . fmap rates)

ratesByUrbanicityAndYear :: FL.Fold MaybeRow (F.FrameRec '[Urbanicity, Year, CrimeRate, IncarcerationRate])
ratesByUrbanicityAndYear =
  let selectMaybe :: MaybeRow -> Maybe (F.Record '[Urbanicity, Year, TotalPop, IndexCrime, TotalPrisonAdm]) = F.recMaybe . F.rcast
      getKey :: F.Record '[Urbanicity, Year, TotalPop, IndexCrime, TotalPrisonAdm] -> F.Record '[Urbanicity, Year] = F.rcast --r = (r ^. state, r ^. year)
      getSubset :: F.Record '[Urbanicity, Year, TotalPop, IndexCrime, TotalPrisonAdm] -> F.Record '[TotalPop, IndexCrime, TotalPrisonAdm]
      getSubset = F.rcast
      addSubset s soFar = ((soFar ^. totalPop) + (s ^. totalPop)) &: ((soFar ^. indexCrime) + (s ^. indexCrime)) &: ((soFar ^. totalPrisonAdm) + (s ^. totalPrisonAdm)) &: V.RNil
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in FL.Fold (aggregateFiltered selectMaybe getKey (addSubset . getSubset) emptyRow) M.empty (F.toFrame . fmap (uncurry V.rappend . second rates) . M.toList) --mapToRecords id . fmap rates)

main :: IO ()
main = do
  config <- D.input D.auto "./config/explore-data.dhall"
  let trendsData = F.readTableMaybe $ trendsCsv config
      analyses = (,)
                 <$> ratesByStateAndYear
                 <*> ratesByUrbanicityAndYear
  (rBySY, rByU) <- F.runSafeEffect $ FL.purely P.fold analyses $ trendsData
  F.writeCSV "data/ratesByStateAndYear.csv" rBySY
  F.writeCSV "data/ratesByUrbanicityAndYear.csv" rByU

--frameToRDataFrame :: R.MonadR m => F.FrameRec rs -> m  


plotUrbanicity :: F.FrameRec '[Urbanicity, Year, CrimeRate, IncarcerationRate] -> IO ()
plotUrbanicity f = do
  let rural :: F.FrameRec '[Year, IncarcerationRate] =  fmap F.rcast $ F.filterFrame (\r -> r ^. urbanicity == "rural") f
      xv :: [Int] = Fold.toList $ fmap (L.view year) rural
      yv :: [Double] =  Fold.toList $ fmap (L.view incarcerationRate) rural
  _ <- R.withEmbeddedR R.defaultConfig $ R.runRegion $ [r| |]
  return ()  

        


{-  
aggregateRecordsFold :: forall as rs ks ds os fs.(Ord (F.Record ks), F.RecVec (ks V.++ fs), as F.⊆ rs, ks F.⊆ as, ds F.⊆ as)
                     => Proxy as
                     -> Proxy ks
                     -> (F.Record ds -> F.Record os -> F.Record os)
                     -> F.Record os
                     -> (F.Record os -> F.Record fs)
                     -> FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (F.FrameRec (ks V.++ fs))
aggregateRecordsFold fieldsProxy keyProxy combine initial extract =
  let selMaybe :: F.Rec (Maybe F.:. F.ElField) rs -> Maybe (F.Record as)
      selMaybe = F.recMaybe . F.rcast
      getKey :: F.Record as -> F.Record ks
      getKey = F.rcast
      getData :: F.Record as -> F.Record ds
      getData = F.rcast
      recombine :: (F.Record ks, F.Record fs) -> F.Record (ks V.++ fs) 
      recombine (x,y) = V.rappend x y
  in FL.Fold (aggregateFiltered selMaybe getKey (combine . getData) initial) M.empty (F.toFrame . fmap (recombine . second extract) . M.toList)


ratesByStateAndYear' :: FL.Fold MaybeRow (F.FrameRec '[State, Year, CrimeRate, IncarcerationRate])
ratesByStateAndYear' =
  let combine :: F.Record [TotalPop,IndexCrime,TotalPrisonAdm] -> F.Record [TotalPop,IndexCrime,TotalPrisonAdm] -> F.Record [TotalPop,IndexCrime,TotalPrisonAdm] 
      combine s soFar = ((soFar ^. totalPop) + (s ^. totalPop)) &: ((soFar ^. indexCrime) + (s ^. indexCrime)) &: ((soFar ^. totalPrisonAdm) + (s ^. totalPrisonAdm)) &: V.RNil
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in aggregateRecordsFold (Proxy @[State,Year,TotalPop,IndexCrime,TotalPrisonAdm]) (Proxy @[State, Year]) combine emptyRow rates
-}

pFilterMaybe :: Monad m => (a -> Maybe b) -> P.Pipe a b m ()
pFilterMaybe f =  P.map f P.>-> P.filter isJust P.>-> P.map fromJust  

maybeTest :: (a -> Bool) -> Maybe a -> Bool
maybeTest t = maybe False t

fipsFilter x = maybeTest (== x) . V.toHKD . F.rget @Fips -- use F.rgetField?
stateFilter s = maybeTest (== s) . V.toHKD . F.rget @State
yearFilter y = maybeTest (== y) . V.toHKD . F.rget @Year

mapToRecords :: Ord k => (k -> (F.Record ks)) -> M.Map k (F.Record cs) -> [F.Record (ks V.++ cs)]
mapToRecords keyToRec = fmap (\(k,dr) -> V.rappend (keyToRec k) dr) . M.toList 

---
