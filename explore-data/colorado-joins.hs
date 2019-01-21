{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE PolyKinds            #-}
{-# LANGUAGE QuasiQuotes          #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Main where

import           DataSources
import qualified Frames.Aggregations        as FA
import qualified Frames.KMeans              as KM
import qualified Frames.MaybeUtils          as FM
import qualified Frames.VegaLite            as FV
import qualified Html.Report                as H


import           Control.Arrow              ((&&&))
import qualified Control.Foldl              as FL
import           Control.Monad.IO.Class     (liftIO)
import           Data.Functor.Identity      (runIdentity)
import qualified Data.List                  as  List
import           Data.Maybe                 (catMaybes)
import           Data.Monoid                ((<>))
import           Data.Proxy                 (Proxy (..))
import           Data.Text                  (Text)
import qualified Data.Text                  as T
import qualified Data.Text.IO               as T
import qualified Data.Text.Lazy             as TL
import qualified Data.Vector                as V
import qualified Data.Vinyl                 as V
import           Data.Vinyl.Curry           (runcurryX)
import qualified Data.Vinyl.Functor         as V
import           Data.Vinyl.Lens            (type (∈))
import           Frames                     ((:.), (&:))
import qualified Frames                     as F
import qualified Frames.CSV                 as F
import qualified Frames.InCore              as FI
import qualified Frames.TH                  as F
import qualified Graphics.Vega.VegaLite     as GV
--import qualified Html                       as H
--import qualified Html.Attribute             as HA
import qualified Pipes                      as P
import qualified Pipes.Prelude              as P
import qualified Lucid                      as HL

-- stage restriction means this all has to be up top
F.tableTypes' (F.rowGen fipsByCountyFP) { F.rowTypeName = "FIPSByCountyRenamed", F.columnNames = ["fips","County","State"]}


type CO_AnalysisVERA_Cols = [Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop]
 
justsFromRec :: V.RMap fs => F.Record fs -> F.Rec (Maybe :. F.ElField) fs
justsFromRec = V.rmap (V.Compose . Just)

type instance FI.VectorFor (Maybe a) = V.Vector
  

main :: IO ()
main = do
  -- create streams which are filtered to CO
  let parserOptions = F.defaultParser { F.quotingMode =  F.RFC4180Quoting ' ' }
      veraData :: F.MonadSafe m => P.Producer (FM.MaybeRow IncarcerationTrends)  m ()
      veraData = F.readTableMaybeOpt F.defaultParser veraTrendsFP  P.>-> P.filter (FA.filterMaybeField (Proxy @State) "CO")
      povertyData :: F.MonadSafe m => P.Producer SAIPE m ()
      povertyData = F.readTableOpt parserOptions censusSAIPE_FP P.>-> P.filter (FA.filterField (Proxy @Abbreviation) "CO")
      fipsByCountyData :: F.MonadSafe m => P.Producer FIPSByCountyRenamed m ()
      fipsByCountyData = F.readTableOpt parserOptions fipsByCountyFP  P.>-> P.filter (FA.filterField (Proxy @State) "CO")
      -- NB: This data has 2 rows per county, one for misdemeanors, one for felonies
      countyBondCO_Data :: F.MonadSafe m => P.Producer (FM.MaybeRow CountyBondCO) m ()
      countyBondCO_Data = F.readTableMaybeOpt parserOptions countyBondCO_FP
      countyDistrictCO_Data :: F.MonadSafe m => P.Producer CountyDistrictCO m ()
      countyDistrictCO_Data = F.readTableOpt parserOptions countyDistrictCrosswalkCO_FP
  -- load streams into memory for joins, subsetting as we go
  fipsByCountyFrame <- F.inCoreAoS $ fipsByCountyData P.>-> P.map (F.rcast @[Fips,County]) -- get rid of state col
  povertyFrame <- F.inCoreAoS $ povertyData P.>-> P.map (F.rcast @[Fips, Year, MedianHI,MedianHIMOE,PovertyR])
  countyBondFrameM <- fmap F.boxedFrame $ F.runSafeEffect $ P.toListM countyBondCO_Data
  veraFrameM <- fmap F.boxedFrame $ F.runSafeEffect $ P.toListM $ veraData P.>-> P.map (F.rcast @[Fips,Year,TotalPop,Urbanicity,IndexCrime])
  countyDistrictFrame <- F.inCoreAoS countyDistrictCO_Data
  putStrLn $ (show $ FL.fold FL.length fipsByCountyFrame) ++ " rows in fipsByCountyFrame."
  putStrLn $ (show $ FL.fold FL.length povertyFrame) ++ " rows in povertyFrame."
  putStrLn $ (show $ FL.fold FL.length countyBondFrameM) ++ " rows in countyBondFrameM."
  putStrLn $ (show $ FL.fold FL.length veraFrameM) ++ " rows in veraFrameM."
  putStrLn $ (show $ FL.fold FL.length countyDistrictFrame) ++ " rows in countyDistrictFrame."
  -- do joins
  let countyBondPlusFIPS = FM.leftJoinMaybe (Proxy @'[County]) countyBondFrameM (justsFromRec <$> fipsByCountyFrame)
      countyBondPlusFIPSAndDistrict = FM.leftJoinMaybe (Proxy @'[County]) (F.boxedFrame countyBondPlusFIPS) (justsFromRec <$> countyDistrictFrame)
      countyBondPlusFIPSAndSAIPE = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndDistrict) (justsFromRec <$> povertyFrame)
      countyBondPlusFIPSAndSAIPEAndVera = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndSAIPE) veraFrameM
  kmMoneyBondPctAnalysis countyBondPlusFIPSAndSAIPEAndVera
--  kmBondRatevsCrimeRateAnalysis countyBondPlusFIPSAndSAIPEAndVera



type IndexCrimeRate = "index_crime_rate" F.:-> Double
type TotalBondRate = "total_bond_rate" F.:-> Double
kmBondRatevsCrimeRateAnalysis joinedData = do
  let mutate :: F.Record [Year,County,Urbanicity,TotalBondFreq,IndexCrime,TotalPop]
             -> F.Record [Year,County,Urbanicity,TotalBondRate,IndexCrimeRate,TotalPop]
      mutate = runcurryX (\y c u tbf ic tp -> y &: c &: u &: (realToFrac tbf/realToFrac ic) &: (realToFrac ic/realToFrac tp) &: tp &: V.RNil)
      mData = fmap mutate . catMaybes $ fmap (F.recMaybe . F.rcast) joinedData
  F.writeCSV "data/raw_COCrimeRatevsBondRate.csv" mData
{-  let dataCols = Proxy @[TotalBondRate, IndexCrimeRate, TotalPop]
      xScaling = FL.premap (F.rgetField @TotalBondRate &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
      yScaling = FL.premap (F.rgetField @IndexCrimeRate &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
-}

--reoffenseAnalysis joinedData = do
  

-- NB: The data has two rows for each county and year, one for misdemeanors and one for felonies.
type MoneyPct = "moneyPct" F.:-> Double --F.declareColumn "moneyPct" ''Double
kmMoneyBondPctAnalysis joinedData = do
  htmlAsText <- H.makeReportHtmlAsText "Colorado Money Bonds vs Personal Recognizance Bonds" $ do
      -- this does two things.  Sets the type for rcast to infer and then mutates it.  Those should likely be separate.
    let mutateData :: F.Record [Year,County,OffType,Urbanicity,PovertyR, MoneyBondFreq,TotalBondFreq,TotalPop]
                   -> F.Record [Year,County,OffType,Urbanicity,PovertyR,MoneyPct,TotalPop]
        mutateData  = runcurryX (\y c ot u pr mbf tbf tp -> y &: c &: ot &: u &: pr &: (realToFrac mbf)/(realToFrac tbf) &: tp &: V.RNil)
         --countyBondPlusFIPSAndSAIPEAndVera
        kmData = fmap mutateData . catMaybes $ fmap (F.recMaybe . F.rcast) joinedData
        dataProxy = Proxy @[PovertyR,MoneyPct,TotalPop]
        -- this rescales both x and y data so they have mean 0, std-dev of 1 and thus k-means will treat both variables as roughly
        -- equivalent when clustering.
        sunXF = FL.premap (F.rgetField @PovertyR &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        sunYF = FL.premap (F.rgetField @MoneyPct &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        kmMoneyBondRatevsPovertyRate proxy_ks = KM.kMeans proxy_ks dataProxy sunXF sunYF 5 KM.partitionCentroids KM.euclidSq        
        (kmByYear, kmByYearUrb) = runIdentity $ FL.foldM ((,)
                                                           <$> kmMoneyBondRatevsPovertyRate (Proxy @[Year,OffType])
                                                           <*> kmMoneyBondRatevsPovertyRate (Proxy @[Year,OffType,Urbanicity])) kmData
    H.placeTextSection $ do
      HL.h1_ "Colorado Money Bond Analysis"
      HL.p_ "Colorado issues two types of bonds when releasing people from jail before trial. Sometimes people are released on a \"money bond\" and sometimes on a personal recognizance bond. We have county-level data of all bonds (?) issued in 2014, 2015 and 2016.  Plotting it all is very noisy so we use a population-weighted k-means clustering technique to combine similar counties into clusters. In the plots below, each circle represents one cluster of counties and the circle's size represents the total population of all the counties included in the cluster.  We consider felonies and misdemeanors separately."
    H.placeVisualization "mBondsVspRateByYrFelonies" $ moneyBondPctVsPovertyRateVL False kmByYear
    H.placeTextSection $ HL.p_ "Broken down by \"urbanicity\":"
    H.placeVisualization "mBondsVspRateByYrUrbFelonies" $ moneyBondPctVsPovertyRateVL True kmByYearUrb
    H.placeTextSection $ HL.p_ "(Denver, the only urban county in CO, didn't report misdemeanors in this data)"
    H.placeTextSection $ do
      HL.h2_ "Some notes on weighted k-means"
      HL.ul_ $ do
        HL.li_ "k-means works by choosing random starting locations for cluster centers, assigning each data-point to the nearest cluster center, moving the center of each cluster to the weighted average of the data-points assigned to the same cluster and then repeating until no data-points move to a new cluster."
        HL.li_ "One subtlety comes around defining distance between points.  Here, before we hand the data off to the k-means algorithm, we shift and rescale the x and y data so each has mean 0 and std-deviation 1.  Then we use ordinary Euclidean distance, that is the sum of the squares of the differences in each coordinate.  Before plotting, we reverse that scaling so that we can visualize the data in the original."
        HL.li_ "This clustering happens for each combination of year and offense type (in the upper plots) and then each combination of year, offense type and urbanicity (in the plots below)."
        

  T.writeFile "analysis/moneyBonds.html" $ TL.toStrict $ htmlAsText

moneyBondPctVsPovertyRateVL facetByUrb dataRecords =
  let dat = FV.recordsToVLData (transformF @MoneyPct (*100)) dataRecords
      enc = GV.encoding
        . GV.position GV.X [FV.pName @PovertyR, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "Poverty Rate (%)"]]
        . GV.position GV.Y [FV.pName @MoneyPct, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "% Money Bonds"]]
        . GV.color [FV.mName @Year, GV.MmType GV.Nominal]
        . GV.size [FV.mName @TotalPop, GV.MmType GV.Quantitative]
        . GV.row [FV.fName @OffType, GV.FmType GV.Nominal, GV.FHeader [GV.HTitle "Type of Offense"]]
        . if facetByUrb then GV.column [FV.fName @Urbanicity, GV.FmType GV.Nominal] else id
      sizing = if facetByUrb
               then [GV.autosize [GV.AFit, GV.AResize]]
               else [GV.autosize [GV.AFit], GV.height 300, GV.width 700]
      vl = GV.toVegaLite $
        [ GV.description "Vega-Lite Attempt"
        , GV.title ("% of money bonds (out of money and personal recognizance bonds) vs poverty rate in CO")
        , GV.background "white"
        , GV.mark GV.Point []
        , enc []
        , dat] <> sizing
  in vl

transformF :: forall x rs. (V.KnownField x, x ∈ rs) => (FA.FType x -> FA.FType x) -> F.Record rs -> F.Record rs
transformF f r = F.rputField @x (f $ F.rgetField @x r) r


--  FM.writeCSV_Maybe "data/countyBondPlusFIPS.csv" countyBondPlusFIPS
--  FM.writeCSV_Maybe "data/countyBondPlusSAIPE.csv" countyBondPlusFIPSAndSAIPE
--  FM.writeCSV_Maybe "data/countyBondPlus.csv" countyBondPlusFIPSAndSAIPEAndVera
--  coloradoRowCheck <- F.runSafeEffect $ FL.purely P.fold (goodDataByKey  [F.pr1|Year|]) coloradoTrendsData
--  putStrLn $ "(CO rows, CO rows with all fields) = " ++ show coloradoRowCheck



