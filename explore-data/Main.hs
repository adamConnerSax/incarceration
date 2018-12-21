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
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeFamilies #-}
module Main where

import           TrendsDataTypes

import qualified Language.R.Instance as R
import Language.R.QQ

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import Control.Applicative (liftA2)
import qualified Data.List            as L
import qualified Data.Map             as M
import           Data.Maybe           (fromJust, fromMaybe, isJust)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry           as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec      as V
import qualified Data.Vinyl.Core as V
import qualified Data.Vinyl.Class.Method as V
import qualified Dhall                as D
import           Frames               ((:.), (&:))
import qualified Frames               as F
import qualified Frames.CSV           as F
import qualified Frames.ShowCSV           as F
import qualified Frames.InCore        as F
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import Control.Arrow (second)
import Data.Proxy (Proxy(..))
import qualified Data.Foldable as Fold
import qualified Data.Vector as V

data Config = Config
  {
    trendsCsv :: FilePath
  , fipsCode  :: Integer
  } deriving (D.Generic)

instance D.Interpret Config

type Row = IncarcerationTrends
type MaybeRow r = F.Rec (Maybe F.:. F.ElField) (F.RecordColumns r)
type MaybeITrends = MaybeRow IncarcerationTrends

data GenderT = Male | Female deriving (Show,Enum,Bounded,Ord, Eq) -- we ought to have NonBinary here as well, but the data doesn't.
type instance F.VectorFor GenderT = V.Vector
instance F.ShowCSV GenderT where
  showCSV = T.pack . show

F.declareColumn "ImprisonedPerCrimeRate" ''Double
F.declareColumn "CrimeRate" ''Double
F.declareColumn "IncarcerationRate" ''Double
F.declareColumn "PrisonAdmRate" '' Double
F.declareColumn "Gender" ''GenderT

-- Utils.  Should get their own lib
aggregateToMap :: Ord k => (a -> k) -> (a -> b -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key = getKey r
      newVal = Just . combine r . fromMaybe initial 
  in M.alter newVal key m --M.insert key newVal m 

-- fold over c.  But c may become zero or many a (bad data, or melting rows). So we process c, then fold over the result.
aggregateGeneral :: (Ord k, Foldable f) => (c -> f a) -> (a -> k) -> (a -> b -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateGeneral unpack getKey combine initial m x =
  let aggregate = FL.Fold (aggregateToMap getKey combine initial) m id
  in FL.fold aggregate (unpack x)

-- Maybe is delightfully foldable!  
aggregateFiltered :: Ord k => (c -> Maybe a) -> (a -> k) -> (a -> b -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateFiltered = aggregateGeneral

-- specific version for our record folds via Control.Foldl
aggregateF :: forall rs ks as bs cs f g. (ks F.⊆ as, Ord (F.Record ks), F.RecVec (ks V.++ cs), Foldable f)
           => Proxy ks
           -> (F.Rec g rs -> f (F.Record as))
           -> (F.Record as -> F.Record bs -> F.Record bs)
           -> F.Record bs
           -> (F.Record bs -> F.Record cs)
           -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateF _ unpack process initial extract =
  let getKey :: F.Record as -> F.Record ks
      getKey = F.rcast
  in FL.Fold (aggregateGeneral unpack getKey process initial) M.empty (F.toFrame . fmap (uncurry V.rappend . second extract) . M.toList) 
--

rates :: F.Record [TotalPop15to64, IndexCrime, TotalPrisonAdm] -> F.Record [CrimeRate, ImprisonedPerCrimeRate]
rates = V.runcurryX (\p ic pa -> (fromIntegral ic / fromIntegral p) &: (fromIntegral pa / fromIntegral ic) &: V.RNil)

-- NB: right now we are choosing to drop any rows which are missing the fields we use.
ratesByStateAndYear :: FL.Fold MaybeITrends (F.FrameRec '[Year, State, CrimeRate, ImprisonedPerCrimeRate])
ratesByStateAndYear =
  let selectMaybe :: MaybeITrends -> Maybe (F.Record '[Year, State, TotalPop15to64, IndexCrime, TotalPrisonAdm]) = F.recMaybe . F.rcast
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in aggregateF (Proxy @[Year, State]) selectMaybe (V.recAdd . F.rcast) emptyRow rates

ratesByUrbanicityAndYear :: FL.Fold MaybeITrends (F.FrameRec '[Urbanicity, Year, CrimeRate, ImprisonedPerCrimeRate])
ratesByUrbanicityAndYear =
  let selectMaybe :: MaybeITrends -> Maybe (F.Record '[Urbanicity, Year, TotalPop15to64, IndexCrime, TotalPrisonAdm]) = F.recMaybe . F.rcast
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in aggregateF (Proxy @[Urbanicity, Year]) selectMaybe (V.recAdd . F.rcast) emptyRow rates

gRates :: F.Record '["pop" F.:-> Int, "adm" F.:-> Int, "inJail" F.:-> Double] -> F.Record '[IncarcerationRate, PrisonAdmRate]
gRates = V.runcurryX (\p a j -> j /(fromIntegral p) F.&: (fromIntegral a/fromIntegral p) F.&: V.RNil)

ratesByGenderAndYear :: FL.Fold MaybeITrends (F.FrameRec '[Year, Gender, IncarcerationRate, PrisonAdmRate])
ratesByGenderAndYear =
  let genders = [Female F.&: V.RNil, Male F.&: V.RNil]
      processRow g r = case (g ^. gender) of
                         Female -> (r ^. femalePop15to64) F.&: (r ^. femalePrisonAdm) F.&: (r ^. femaleJailPop) F.&: V.RNil
                         Male ->  (r ^. malePop15to64) F.&: (r ^. malePrisonAdm) F.&: (r ^. maleJailPop) F.&: V.RNil
      newRows = reshapeRowSimple ([F.pr1|Year|]) genders processRow
      selectMaybe :: MaybeITrends -> Maybe (F.Record '[Year, FemalePop15to64, MalePop15to64, FemalePrisonAdm, MalePrisonAdm, FemaleJailPop, MaleJailPop])
      selectMaybe = F.recMaybe . F.rcast
      unpack :: MaybeITrends -> [F.Record '[Year, Gender, "pop" F.:-> Int, "adm" F.:-> Int, "inJail" F.:-> Double]]
      unpack = fromMaybe [] . fmap newRows . selectMaybe
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in aggregateF (Proxy @[Year, Gender]) unpack (V.recAdd . F.rcast) emptyRow gRates

main :: IO ()
main = do
  config <- D.input D.auto "./config/explore-data.dhall"
  let trendsData = F.readTableMaybe $ trendsCsv config
      analyses = (,,)
                 <$> ratesByStateAndYear
                 <*> ratesByUrbanicityAndYear
                 <*> ratesByGenderAndYear
  (rBySY, rByU, rByG) <- F.runSafeEffect $ FL.purely P.fold analyses $ trendsData
  F.writeCSV "data/ratesByStateAndYear.csv" rBySY
  F.writeCSV "data/ratesByUrbanicityAndYear.csv" rByU
  F.writeCSV "data/ratesByGenderAndYear.csv" rByG


reshapeRowSimple :: forall ss ts cs ds. (ss F.⊆ ts)
                 => Proxy ss -- id columns
                 -> [F.Record cs] -- list of classifier values
                 -> (F.Record cs -> F.Record ts -> F.Record ds)
                 -> F.Record ts
                 -> [F.Record (ss V.++ cs V.++ ds)]                
reshapeRowSimple _ classifiers newDataF r = 
  let ids = F.rcast r :: F.Record ss
  in flip fmap classifiers $ \c -> (ids F.<+> c) F.<+> newDataF c r  


{-
--frameToRDataFrame :: R.MonadR m => F.FrameRec rs -> m  
plotUrbanicity :: F.FrameRec '[Urbanicity, Year, CrimeRate, IncarcerationRate] -> IO ()
plotUrbanicity f = do
  let rural :: F.FrameRec '[Year, IncarcerationRate] =  fmap F.rcast $ F.filterFrame (\r -> r ^. urbanicity == "rural") f
      xv :: [Int] = Fold.toList $ fmap (L.view year) rural
      yv :: [Double] =  Fold.toList $ fmap (L.view incarcerationRate) rural
  _ <- R.withEmbeddedR R.defaultConfig $ R.runRegion $ [r| |]
  return ()  
-}
        


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
  let combine :: F.Record [TotalPop15to64,IndexCrime,TotalPrisonAdm] -> F.Record [TotalPop15to64,IndexCrime,TotalPrisonAdm] -> F.Record [TotalPop15to64,IndexCrime,TotalPrisonAdm] 
      combine s soFar = ((soFar ^. totalPop) + (s ^. totalPop)) &: ((soFar ^. indexCrime) + (s ^. indexCrime)) &: ((soFar ^. totalPrisonAdm) + (s ^. totalPrisonAdm)) &: V.RNil
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in aggregateRecordsFold (Proxy @[State,Year,TotalPop15to64,IndexCrime,TotalPrisonAdm]) (Proxy @[State, Year]) combine emptyRow rates
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
