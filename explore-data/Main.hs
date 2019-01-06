{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
module Main where

import           Frames.Aggregations     as FA
import           TrendsDataTypes

import qualified Control.Foldl           as FL
import           Control.Lens            ((^.))
import           Control.Monad.IO.Class  (MonadIO, liftIO)
import qualified Data.List               as List
import qualified Data.Map                as M
import           Data.Maybe              (catMaybes, fromMaybe, isJust)
import           Data.Proxy              (Proxy (..))
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Vector             as V
import qualified Data.Vinyl              as V
import qualified Data.Vinyl.Class.Method as V
import           Data.Vinyl.Curry        (runcurryX)
import           Data.Vinyl.XRec         as V
import           Frames                  ((:.), (&:))
import qualified Frames                  as F
import qualified Frames.CSV              as F
import qualified Frames.InCore           as FI
import qualified Frames.ShowCSV          as F
import qualified Pipes                   as P
import qualified Pipes.Prelude           as P

{-
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
-}
rates :: F.Record [TotalPop15to64, IndexCrime, TotalPrisonAdm] -> F.Record [CrimeRate, ImprisonedPerCrimeRate]
rates = runcurryX (\p ic pa -> (fromIntegral ic / fromIntegral p) &: (fromIntegral pa / fromIntegral ic) &: V.RNil)

-- NB: right now we are choosing to drop any rows which are missing the fields we use.
ratesByStateAndYear :: FL.Fold MaybeITrends (F.FrameRec '[Year, State, CrimeRate, ImprisonedPerCrimeRate])
ratesByStateAndYear =
  let selectMaybe :: MaybeITrends -> Maybe (F.Record '[Year, State, TotalPop15to64, IndexCrime, TotalPrisonAdm]) = F.recMaybe . F.rcast
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in FA.aggregateF (Proxy @[Year, State]) selectMaybe (flip $ V.recAdd . F.rcast) emptyRow rates

ratesByUrbanicityAndYear :: FL.Fold MaybeITrends (F.FrameRec '[Urbanicity, Year, CrimeRate, ImprisonedPerCrimeRate])
ratesByUrbanicityAndYear =
  let selectMaybe :: MaybeITrends -> Maybe (F.Record '[Urbanicity, Year, TotalPop15to64, IndexCrime, TotalPrisonAdm]) = F.recMaybe . F.rcast
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in FA.aggregateF (Proxy @[Urbanicity, Year]) selectMaybe (flip $ V.recAdd . F.rcast) emptyRow rates

type CO_AnalysisVERA_Cols = [Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop]

--jailDataByStateAndYear :: FL.Fold MaybeITrends (F.Record '[Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop])

gRates :: F.Record '["pop" F.:-> Int, "adm" F.:-> Int, "inJail" F.:-> Double] -> F.Record '[IncarcerationRate, PrisonAdmRate]
gRates = runcurryX (\p a j -> j /(fromIntegral p) F.&: (fromIntegral a/fromIntegral p) F.&: V.RNil)

ratesByGenderAndYear :: FL.Fold MaybeITrends (F.FrameRec '[Year, Gender, IncarcerationRate, PrisonAdmRate])
ratesByGenderAndYear =
  let genders = [Female F.&: V.RNil, Male F.&: V.RNil]
      processRow g r = case (g ^. gender) of
                         Female -> (r ^. femalePop15to64) F.&: (r ^. femalePrisonAdm) F.&: (r ^. femaleJailPop) F.&: V.RNil
                         Male ->  (r ^. malePop15to64) F.&: (r ^. malePrisonAdm) F.&: (r ^. maleJailPop) F.&: V.RNil
      newRows = FA.reshapeRowSimple proxyYear genders processRow
      selectMaybe :: MaybeITrends -> Maybe (F.Record '[Year, FemalePop15to64, MalePop15to64, FemalePrisonAdm, MalePrisonAdm, FemaleJailPop, MaleJailPop])
      selectMaybe = F.recMaybe . F.rcast
      unpack :: MaybeITrends -> [F.Record '[Year, Gender, "pop" F.:-> Int, "adm" F.:-> Int, "inJail" F.:-> Double]]
      unpack = fromMaybe [] . fmap newRows . selectMaybe
      emptyRow = 0 &: 0 &: 0 &: V.RNil
  in FA.aggregateF (Proxy @[Year, Gender]) unpack (flip $ V.recAdd . F.rcast) emptyRow gRates



type TrendsPovRow = '[Year, Fips, State, CountyName, Urbanicity, TotalPop, CrimeRate, IncarcerationRate, ImprisonedPerCrimeRate]
--TotalPop, TotalPop15To64, TotalPrisonPop, TotalPrisonAdm, IndexCrime]

trendsRowForPovertyAnalysis :: Monad m => P.Pipe MaybeITrends (F.Record TrendsPovRow) m ()
trendsRowForPovertyAnalysis = do
  r <- P.await
  let dataWeNeedM :: Maybe (F.Rec F.ElField '[Year, Fips, State, CountyName, Urbanicity, TotalPop, TotalPop15to64, TotalPrisonPop, TotalPrisonAdm, IndexCrime]) = F.recMaybe $ F.rcast r
  case dataWeNeedM of
    Nothing -> trendsRowForPovertyAnalysis
    Just x -> do
      let newRow = runcurryX (\y f s c u tp tp' tpp tpa ic -> y &: f &: s &: c &: u &: tp &: (fromIntegral ic/fromIntegral tp) &: (fromIntegral tpp/fromIntegral tp) &: (if ic == 0 then 0 :: Double else (fromIntegral tpa/fromIntegral ic)) &: V.RNil) x
      P.yield newRow >> trendsRowForPovertyAnalysis

main :: IO ()
main = do
  let trendsData :: F.MonadSafe m => P.Producer MaybeITrends m ()
      trendsData = F.readTableMaybe incarcerationTrendsCsvPath -- some data is missing so we use the Maybe form
--  let povertyData = F.readTable saipeCsvPath
--  let fipsByCountyData = F.readTable $ fipsByCountyCsv config
--  let crimeStatsCO_Data = F.readTable crimeStatsCO_CsvPath
  let coloradoTrends = trendsData P.>-> P.map (F.rcast @CO_AnalysisVERA_Cols) P.>-> P.filter (stateFilter "CO")
--      goodDataByYear = FL.Fold (aggregateToMap (F.rcast @'[Year]) (flip (:)) []) M.empty (fmap $ FL.fold goodDataCount)
  coloradoRowCheck <- F.runSafeEffect $ FL.purely P.fold (goodDataByKey  [F.pr1|Year|]) coloradoTrends
  putStrLn $ "(CO rows, CO rows with all fields) = " ++ show coloradoRowCheck
    --aggregationAnalyses trendsData
  --incomePovertyJoinData trendsData povertyData

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


incomePovertyJoinData trendsData povertyData = do
  trendsForPovFrame <- F.inCoreAoS $ trendsData P.>-> trendsRowForPovertyAnalysis
  povertyFrame :: F.Frame SAIPE <- F.inCoreAoS povertyData
  let trendsWithPovertyF = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[Fips,Year] trendsForPovFrame povertyFrame
  F.writeCSV "data/trendsWithPoverty.csv" trendsWithPovertyF
  let dataProxy = Proxy @[MedianHI, IncarcerationRate, TotalPop]
      smIncarcerationRatevsIncome proxy_ks = scatterMerge proxy_ks dataProxy round id 10 10 RescaleMedian RescaleNone
      (smYrFrame, smStateYrFrame, smUrbYrFrame) = FL.fold ((,,)
                                                           <$> smIncarcerationRatevsIncome proxyYear
                                                           <*> smIncarcerationRatevsIncome (Proxy @[State,Year])
                                                           <*> smIncarcerationRatevsIncome (Proxy @[Urbanicity,Year])) trendsWithPovertyF
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByYear.csv" smYrFrame
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByStateAndYear.csv" smStateYrFrame
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByUrbanicityAndYear.csv" smUrbYrFrame

{-
pFilterMaybe :: Monad m => (a -> Maybe b) -> P.Pipe a b m ()
pFilterMaybe f =  P.map f P.>-> P.filter isJust P.>-> P.map fromJust
-}

maybeTest :: (a -> Bool) -> Maybe a -> Bool
maybeTest t = maybe False t

fipsFilter x = maybeTest (== x) . V.toHKD . F.rget @Fips -- use F.rgetField?
stateFilter s = maybeTest (== s) . V.toHKD . F.rget @State
yearFilter y = maybeTest (== y) . V.toHKD . F.rget @Year
