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
--{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Main where

import           DataSources
import qualified Frames.Folds                  as FF
import qualified Frames.Utils                  as FU
import qualified Frames.Transform              as FT
import qualified Frames.MapReduce              as MR
import qualified Frames.KMeans                 as KM
import qualified Math.Rescale                  as MR

import qualified Knit.Effect.Logger            as KL
import           Polysemy                       ( runM
                                                , Semantic
                                                , Lift
                                                )
--import qualified Control.Monad.Freer           as FR

import qualified Control.Foldl                 as FL
import           Control.Lens                   ( (^.) )
import           Control.Monad.IO.Class         ( liftIO )
import           Data.Maybe                     ( catMaybes
                                                , fromMaybe
                                                )
import qualified Data.Text                     as T
import qualified Data.Vinyl                    as V
import           Data.Vinyl.Curry               ( runcurryX )
import           Data.Vinyl.XRec               as V
import           Frames                         ( (:.)
                                                , (&:)
                                                )
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

rates
  :: F.Record '[TotalPop15to64, IndexCrime, TotalPrisonAdm]
  -> F.Record '[CrimeRate, ImprisonedPerCrimeRate]
rates = runcurryX
  (\p ic pa ->
    (fromIntegral ic / fromIntegral p)
      &: (fromIntegral pa / fromIntegral ic)
      &: V.RNil
  )

-- NB: right now we are choosing to drop any rows which are missing the fields we use.
ratesByStateAndYear
  :: FL.Fold
       MaybeITrends
       (F.FrameRec '[Year, State, CrimeRate, ImprisonedPerCrimeRate])
ratesByStateAndYear = MR.concatFold $ MR.mapReduceFold
  (MR.unpackGoodRows @'[Year, State, TotalPop15to64, IndexCrime, TotalPrisonAdm]
  )
  (MR.splitOnKeys @'[Year, State])
  (MR.foldAndAddKey $ fmap rates $ FF.foldAllConstrained @Num FL.sum)

ratesByUrbanicityAndYear
  :: FL.Fold
       MaybeITrends
       (F.FrameRec '[Urbanicity, Year, CrimeRate, ImprisonedPerCrimeRate])
ratesByUrbanicityAndYear = MR.concatFold $ MR.mapReduceFold
  (MR.unpackGoodRows
    @'[Urbanicity, Year, TotalPop15to64, IndexCrime, TotalPrisonAdm]
  )
  (MR.splitOnKeys @'[Urbanicity, Year])
  (MR.foldAndAddKey $ fmap rates $ FF.foldAllConstrained @Num FL.sum)

type CO_AnalysisVERA_Cols = [Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop]


gRates
  :: F.Record '["pop" F.:-> Int, "adm" F.:-> Int, "inJail" F.:-> Double]
  -> F.Record '[IncarcerationRate, PrisonAdmRate]
gRates = runcurryX
  (\p a j ->
    j / (fromIntegral p) F.&: (fromIntegral a / fromIntegral p) F.&: V.RNil
  )

ratesByGenderAndYear
  :: FL.Fold
       MaybeITrends
       (F.FrameRec '[Year, Gender, IncarcerationRate, PrisonAdmRate])
ratesByGenderAndYear = MR.concatFold $ MR.mapReduceFold
  unpack
  (MR.splitOnKeys @'[Year, Gender])
  (MR.foldAndAddKey $ fmap gRates $ FF.foldAllConstrained @Num FL.sum)
 where
  genders = [Female F.&: V.RNil, Male F.&: V.RNil]
  processRow
    :: F.Record '[Gender]
    -> F.Record
         '[Year, FemalePop15to64, MalePop15to64, FemalePrisonAdm, MalePrisonAdm, FemaleJailPop, MaleJailPop]
    -> F.Record '["pop" F.:-> Int, "adm" F.:-> Int, "inJail" F.:-> Double]
  processRow g r = case (g ^. gender) of
    Female ->
      (r ^. femalePop15to64)
        F.&: (r ^. femalePrisonAdm)
        F.&: (r ^. femaleJailPop)
        F.&: V.RNil
    Male ->
      (r ^. malePop15to64)
        F.&: (r ^. malePrisonAdm)
        F.&: (r ^. maleJailPop)
        F.&: V.RNil
  newRows = FT.reshapeRowSimple @'[Year] genders processRow
  unpack =
    MR.Unpack
      $ fromMaybe []
      . fmap newRows
      . F.recMaybe
      . F.rcast
        @'[Year, FemalePop15to64, MalePop15to64, FemalePrisonAdm, MalePrisonAdm, FemaleJailPop, MaleJailPop]

type TrendsPovRow = '[Year, Fips, State, CountyName, Urbanicity, TotalPop, CrimeRate, IncarcerationRate, ImprisonedPerCrimeRate]


trendsRowForPovertyAnalysis
  :: Monad m => P.Pipe MaybeITrends (F.Record TrendsPovRow) m ()
trendsRowForPovertyAnalysis = do
  r <- P.await
  let
    dataWeNeedM :: Maybe
          ( F.Rec
              F.ElField
              '[Year, Fips, State, CountyName, Urbanicity, TotalPop, TotalPop15to64, TotalPrisonPop, TotalPrisonAdm, IndexCrime]
          )
      = F.recMaybe $ F.rcast r
  case dataWeNeedM of
    Nothing -> trendsRowForPovertyAnalysis
    Just x  -> do
      let
        newRow = runcurryX
          (\y f s c u tp tp' tpp tpa ic ->
            y
              &: f
              &: s
              &: c
              &: u
              &: tp
              &: (fromIntegral ic / fromIntegral tp)
              &: (fromIntegral tpp / fromIntegral tp)
              &: (if ic == 0
                   then 0 :: Double
                   else (fromIntegral tpa / fromIntegral ic)
                 )
              &: V.RNil
          )
          x
      P.yield newRow >> trendsRowForPovertyAnalysis

main :: IO ()
main = do
  let trendsData :: F.MonadSafe m => P.Producer MaybeITrends m ()
      trendsData = F.readTableMaybe veraTrendsFP -- some data is missing so we use the Maybe form
  let povertyData = F.readTable censusSAIPE_FP
  runM $ KL.filteredLogEntriesToIO KL.logAll $ do
    incomePovertyJoinData trendsData povertyData

aggregationAnalyses
  :: P.Producer
       (F.Rec (Maybe :. F.ElField) (F.RecordColumns IncarcerationTrends))
       (P.Effect (F.SafeT IO))
       ()
  -> IO ()
aggregationAnalyses trendsData = do
  let analyses =
        (,,)
          <$> ratesByStateAndYear
          <*> ratesByUrbanicityAndYear
          <*> ratesByGenderAndYear
  (rBySY, rByU, rByG) <-
    F.runSafeEffect $ FL.purely P.fold analyses $ trendsData
  liftIO $ F.writeCSV "data/ratesByStateAndYear.csv" rBySY
  liftIO $ F.writeCSV "data/ratesByUrbanicityAndYear.csv" rByU
  liftIO $ F.writeCSV "data/ratesByGenderAndYear.csv" rByG

type X = "X" F.:-> Double
type Y = "Y" F.:-> Double

type M = Semantic '[KL.Logger KL.LogEntry, KL.PrefixLog, Lift IO]

incomePovertyJoinData trendsData povertyData = do
  trendsForPovFrame <-
    liftIO @M $ F.inCoreAoS $ trendsData P.>-> trendsRowForPovertyAnalysis
  povertyFrame :: F.Frame SAIPE <- liftIO $ F.inCoreAoS povertyData
  let trendsWithPovertyF =
        F.leftJoin @'[Fips, Year] trendsForPovFrame povertyFrame
  liftIO
    $ F.writeCSV "data/trendsWithPoverty.csv"
    $ F.toFrame
    $ catMaybes
    $ fmap F.recMaybe
    $ trendsWithPovertyF
  let
    sunXF :: FL.Fold (F.Record '[MedianHI, TotalPop]) (MR.ScaleAndUnscale Int)
    sunXF = FL.premap (runcurryX (\x w -> (x, w))) $ MR.weightedScaleAndUnscale
      (MR.RescaleMedian 100)
      (MR.RescaleMedian 100)
      round
    sunYF
      :: FL.Fold
           (F.Record '[IncarcerationRate, TotalPop])
           (MR.ScaleAndUnscale Double)
    sunYF = FL.premap (runcurryX (\y w -> (y, w))) $ MR.weightedScaleAndUnscale
      (MR.RescaleGiven (0, 0.01))
      MR.RescaleNone
      id
    kmReduce
      :: MR.ReduceM
           M
           k
           (F.Record '[MedianHI, IncarcerationRate, TotalPop])
           [(Int, Double, Int)]
    kmReduce =
      MR.ReduceM $ \_ -> KM.kMeansOne @MedianHI @IncarcerationRate @TotalPop
        sunXF
        sunYF
        10
        (\n -> return . KM.partitionCentroids @X @Y n)
        (KM.weighted2DRecord @X @Y @TotalPop)
        KM.euclidSq
    toRec (x, y, z) =
      (x F.&: y F.&: z F.&: V.RNil) :: F.Record
          '[MedianHI, IncarcerationRate, TotalPop]
    kmFold assign = MR.concatFoldM $ MR.mapReduceFoldM
      ( MR.generalizeUnpack
      $ MR.unpackGoodRows
        @'[Year, State, Urbanicity, MedianHI, IncarcerationRate, TotalPop]
      )
      (MR.generalizeAssign assign)
      (MR.makeRecsWithKeyM toRec kmReduce)
  (kmYrFrame, kmStateYrFrame, kmUrbYrFrame) <- FL.foldM
    (   (,,)
    <$> kmFold
          (MR.assignKeysAndData @'[Year]
            @'[MedianHI, IncarcerationRate, TotalPop]
          )
    <*> kmFold
          (MR.assignKeysAndData @'[State, Year]
            @'[MedianHI, IncarcerationRate, TotalPop]
          )
    <*> kmFold
          (MR.assignKeysAndData @'[Urbanicity, Year]
            @'[MedianHI, IncarcerationRate, TotalPop]
          )
    )
    trendsWithPovertyF
  liftIO $ do
    F.writeCSV "data/kMeansIncarcerationRate_vs_MedianHIByYear.csv" kmYrFrame
    F.writeCSV "data/kMeansIncarcerationRate_vs_MedianHIByStateAndYear.csv"
               kmStateYrFrame
    F.writeCSV
      "data/kMeansIncarcerationRate_vs_MedianHIByUrbanicityAndYear.csv"
      kmUrbYrFrame


maybeTest :: (a -> Bool) -> Maybe a -> Bool
maybeTest t = maybe False t

fipsFilter x = maybeTest (== x) . V.toHKD . F.rget @Fips -- use F.rgetField?
stateFilter s = maybeTest (== s) . V.toHKD . F.rget @State
yearFilter y = maybeTest (== y) . V.toHKD . F.rget @Year
