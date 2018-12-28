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

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Applicative (liftA2)
import qualified Data.List            as L
import qualified Data.Map             as M
import           Data.Maybe           (fromJust, fromMaybe, isJust, catMaybes)
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
import qualified Frames.InCore        as FI
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import Control.Arrow (second)
import Data.Proxy (Proxy(..))
import qualified Data.Foldable as Fold
import qualified Data.Vector as V

data Config = Config
  {
    trendsCsv :: FilePath
  , povertyCsv :: FilePath
  } deriving (D.Generic)

instance D.Interpret Config

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
aggregateF :: forall rs ks as bs cs f g. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
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

type TrendsPovRow = '[Year, Fips, State, CountyName, CrimeRate, IncarcerationRate, ImprisonedPerCrimeRate]
--TotalPop, TotalPop15To64, TotalPrisonPop, TotalPrisonAdm, IndexCrime]

trendsRowForPovertyAnalysis :: Monad m => P.Pipe MaybeITrends (F.Rec F.ElField TrendsPovRow) m ()
trendsRowForPovertyAnalysis = do
  r <- P.await
  let dataWeNeedM :: Maybe (F.Rec F.ElField '[Year, Fips, State, CountyName, TotalPop, TotalPop15to64, TotalPrisonPop, TotalPrisonAdm, IndexCrime]) = F.recMaybe $ F.rcast r
  case dataWeNeedM of
    Nothing -> trendsRowForPovertyAnalysis
    Just x -> do
      let newRow = V.runcurryX (\y f s c tp tp' tpp tpa ic -> y &: f &: s &: c &: (fromIntegral ic/fromIntegral tp) &: (fromIntegral tpp/fromIntegral tp) &: (fromIntegral tpa/fromIntegral ic) &: V.RNil) x  
      P.yield newRow >> trendsRowForPovertyAnalysis

main :: IO ()
main = do
  config <- D.input D.auto "./config/explore-data.dhall"
  let trendsData = F.readTableMaybe $ trendsCsv config
      povertyData = F.readTable $ povertyCsv config -- no missing data here
--  aggregationAnalyses trendsData
  trendsForPovFrame <- F.inCoreAoS $ trendsData P.>-> trendsRowForPovertyAnalysis
  povertyFrame :: F.Frame SAIPE <- F.inCoreAoS povertyData
  let trendsWithPovertyF = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[Fips,Year] trendsForPovFrame povertyFrame
  F.writeCSV "data/trendsWithPoverty.csv" trendsWithPovertyF    

aggregationAnalyses :: P.Producer (F.Rec (Maybe :. F.ElField) (F.RecordColumns IncarcerationTrends)) (P.Effect (F.SafeT IO)) () -> IO ()
aggregationAnalyses trendsData = do
  let analyses = (,,)
                 <$> ratesByStateAndYear
                 <*> ratesByUrbanicityAndYear
                 <*> ratesByGenderAndYear
  (rBySY, rByU, rByG) <- F.runSafeEffect $ FL.purely P.fold analyses $ trendsData
  liftIO $ F.writeCSV "data/ratesByStateAndYear.csv" rBySY
  liftIO $ F.writeCSV "data/ratesByUrbanicityAndYear.csv" rByU
  liftIO $ F.writeCSV "data/ratesByGenderAndYear.csv" rByG


-- This is the anamorphic step.  Is it a co-algebra of []?
-- You could also use meltRow here.  That is also (Record as -> [Record bs])
reshapeRowSimple :: forall ss ts cs ds. (ss F.⊆ ts)
                 => Proxy ss -- id columns
                 -> [F.Record cs] -- list of classifier values
                 -> (F.Record cs -> F.Record ts -> F.Record ds)
                 -> F.Record ts
                 -> [F.Record (ss V.++ cs V.++ ds)]                
reshapeRowSimple _ classifiers newDataF r = 
  let ids = F.rcast r :: F.Record ss
  in flip fmap classifiers $ \c -> (ids F.<+> c) F.<+> newDataF c r  

--
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
