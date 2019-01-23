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
import           Control.Lens ((%~),(^.))
import           Data.Functor.Identity      (runIdentity)
import qualified Data.List                  as  List
import           Data.Maybe                 (catMaybes, fromMaybe)
import           Data.Monoid                ((<>))
import           Data.Proxy                 (Proxy (..))
import           Data.Text                  (Text)
import qualified Data.Text                  as T
import qualified Data.Text.IO               as T
import qualified Data.Text.Lazy             as TL
import qualified Data.Vector                as V
import qualified Data.Vinyl                 as V
import qualified Data.Vinyl.XRec            as V
import           Data.Vinyl.Curry           (runcurryX)
import qualified Data.Vinyl.Functor         as V
import qualified Data.Vinyl.Class.Method    as V
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
      -- This data has 3 rows per county and year, one for each type of crime (against persons, against property, against society)
      crimeStatsCO_Data :: F.MonadSafe m => P.Producer (FM.MaybeRow CrimeStatsCO) m ()
      crimeStatsCO_Data = F.readTableMaybeOpt parserOptions crimeStatsCO_FP
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
  bondVsCrimeAnalysis countyBondCO_Data crimeStatsCO_Data 

type MoneyBondRate = "money_bond_rate" F.:-> Double
type PostedBondRate = "posted_bond_rate" F.:-> Double
type CRate = "crime_rate" F.:-> Double
bondVsCrimeAnalysis :: P.Producer (FM.MaybeRow CountyBondCO) (F.SafeT IO) () -> P.Producer (FM.MaybeRow CrimeStatsCO) (F.SafeT IO) () -> IO ()
bondVsCrimeAnalysis bondDataMaybeProducer crimeDataMaybeProducer = do
  putStrLn "Doing bond rate vs crime rate analysis..."
  countyBondFrameM <- fmap F.boxedFrame $ F.runSafeT $ P.toListM bondDataMaybeProducer
  let blanksToZeroes :: F.Rec (Maybe :. F.ElField) '[Crimes,Offenses] -> F.Rec (Maybe :. F.ElField) '[Crimes,Offenses]
      blanksToZeroes = FM.fromMaybeMono 0
  -- turn Nothing into 0, sequence the Maybes out, use concat to "fold" over those maybes, removing any nothings
  crimeStatsList <- F.runSafeT $ P.toListM $ crimeDataMaybeProducer P.>-> P.map (F.recMaybe . (F.rsubset %~ blanksToZeroes)) P.>-> P.concat
  let mergeCrimeTypes :: F.Record '[Crimes,Offenses,EstPopulation] -> CrimeStatsCO -> F.Record '[Crimes, Offenses, EstPopulation] 
      mergeCrimeTypes soFar = runcurryX (\c o p -> (soFar ^. crimes + c) &: (soFar ^. offenses) + o &: p &: V.RNil) . F.rcast @[Crimes,Offenses,EstPopulation]
      foldAllCrimes :: FL.Fold (F.Record (F.RecordColumns CrimeStatsCO)) (F.FrameRec [County,Year,Crimes,Offenses,EstPopulation])
      foldAllCrimes = FA.aggregateFs (Proxy @[County,Year]) V.Identity mergeCrimeTypes (0 &: 0 &: 0 &: V.RNil) V.Identity         
      mergedCrimeStatsFrame = FL.fold foldAllCrimes crimeStatsList
      foldAllBonds :: FL.Fold (FM.MaybeRow CountyBondCO) (F.FrameRec [County,Year,MoneyBondFreq,TotalBondFreq,MoneyPosted,PrPosted])
      foldAllBonds = FA.aggregateFs (Proxy @[County,Year]) selectMaybe addBonds (0 &: 0 &: 0 &: 0 &: V.RNil) V.Identity where
        selectMaybe = F.recMaybe . F.rcast @[County,Year,MoneyBondFreq,TotalBondFreq,MoneyPosted,PrPosted]
        addBonds = flip $ V.recAdd . F.rcast @[MoneyBondFreq,TotalBondFreq,MoneyPosted,PrPosted]
      mergedBondDataFrame = FL.fold foldAllBonds countyBondFrameM
      countyBondAndCrime = F.leftJoin @[County,Year] mergedBondDataFrame mergedCrimeStatsFrame
  putStrLn $ (show $ FL.fold FL.length crimeStatsList) ++ " rows in crimeStatsList (unmerged)."
  putStrLn $ (show $ FL.fold FL.length mergedCrimeStatsFrame) ++ " rows in crimeStatsFrame(merged)."
  let kmMoneyBondRatevsCrimeRateByYear = runIdentity $ FL.foldM (kmMoneyBondRatevsCrimeRate (Proxy @'[Year])) kmData where        
        mutateData :: F.Record [Year,County,MoneyBondFreq,TotalBondFreq,Crimes,Offenses,EstPopulation]
                   -> F.Record [Year,County,MoneyBondRate,CRate,EstPopulation]
        mutateData = runcurryX (\yr cnty mbonds tbonds crimes offenses pop ->
                                  case tbonds of
                                    0 -> yr &: cnty &: 0 &: (realToFrac crimes/ realToFrac pop) &: pop &: V.RNil
                                    _ -> yr &: cnty &: (realToFrac mbonds/realToFrac tbonds) &: (realToFrac crimes/ realToFrac pop) &: pop &: V.RNil
                               )
        kmData = fmap mutateData . catMaybes $ fmap (F.recMaybe . F.rcast) countyBondAndCrime
        dataProxy = Proxy @[MoneyBondRate, CRate, EstPopulation]
        sunXF = FL.premap (F.rgetField @CRate &&& F.rgetField @EstPopulation) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        sunYF = FL.premap (F.rgetField @MoneyBondRate &&& F.rgetField @EstPopulation) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        kmMoneyBondRatevsCrimeRate proxy_ks = KM.kMeans proxy_ks dataProxy sunXF sunYF 10 KM.partitionCentroids KM.euclidSq
  let kmPostedBondRatevsCrimeRateByYear = runIdentity $ FL.foldM (kmPostedBondRatevsCrimeRate (Proxy @'[Year])) kmData where        
        mutateData :: F.Record [Year,County,TotalBondFreq,MoneyPosted,PrPosted,Crimes,Offenses,EstPopulation]
                   -> F.Record [Year,County,PostedBondRate,CRate,EstPopulation]
        mutateData = runcurryX (\yr cnty tbonds mposted prposted crimes offenses pop ->
                                  case tbonds of
                                    0 -> yr &: cnty &: 0 &: (realToFrac crimes/ realToFrac pop) &: pop &: V.RNil
                                    _ -> yr &: cnty &: (realToFrac (mposted+prposted)/realToFrac tbonds) &: (realToFrac crimes/ realToFrac pop) &: pop &: V.RNil
                               )
        kmData = fmap mutateData . catMaybes $ fmap (F.recMaybe . F.rcast) countyBondAndCrime
        dataProxy = Proxy @[PostedBondRate, CRate, EstPopulation]
        sunXF = FL.premap (F.rgetField @CRate &&& F.rgetField @EstPopulation) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        sunYF = FL.premap (F.rgetField @PostedBondRate &&& F.rgetField @EstPopulation) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        kmPostedBondRatevsCrimeRate proxy_ks = KM.kMeans proxy_ks dataProxy sunXF sunYF 10 KM.partitionCentroids KM.euclidSq        
  htmlAsText <- H.makeReportHtmlAsText "Colorado Money Bond Rate vs Crime rate" $ do
    H.placeTextSection $ do
      HL.h2_ "Colorado Bond Rates and Crime Rates (preliminary)"
      HL.p_ [HL.class_ "subtitle"] "Adam Conner-Sax"
      HL.p_ "Each county in Colorado issues money bonds and personal recognizance bonds.  For each county I look at the % of money bonds out of all bonds issued and the crime rate.  We have 3 years of data and there are 64 counties in Colorado (each with vastly different populations).  So I've used a population-weighted k-means clustering technique to reduce the number of points to at most 7 per year. Each circle in the plot below represents one cluster of counties with similar money bond and poverty rates.  The size of the circle represents the total population in the cluster."
    H.placeVisualization "crimeRateVsMoneyBondRate" $ moneyBondRateVsCrimeRateVL kmMoneyBondRatevsCrimeRateByYear
    H.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ $ do
          HL.span_ "Money Bond Rate is (number of money bonds/total number of bonds). That data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv."
        HL.li_ $ do
          HL.span_ "Crime Rate is crimes/estimated_population. Those numbers come from the Colorado crime statistics "
          HL.a_ [HL.href_ "https://coloradocrimestats.state.co.us/"] "web-site"
          HL.span_ ".  We also have some of that data in the "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/crime-rate-bycounty.csv"] "repo"
          HL.span_ ", but only for 2016, so I re-downloaded it for this. Also, there are some crimes in that data that don't roll up to a particular county but instead are attributed to the Colorado Bureau of Investigation or the Colorado State Patrol.  I've ignored those."
    H.placeTextSection $ do
      HL.p_ "Below I look at the percentage of all bonds which are \"posted\" (posting bond means paying the money bond or agreeing to the terms of the personal recognizance bond ?) rather than the % of money bonds. I use the same clustering technique."
    H.placeVisualization "crimeRateVsPostedBondRate" $ postedBondRateVsCrimeRateVL kmPostedBondRatevsCrimeRateByYear
    H.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ $ do
          HL.span_ "Posted Bond Rate is [(number of posted money bonds + number of posted PR bonds)/total number of bonds] where that data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv."
        HL.li_ $ do
          HL.span_ "Crime Rate, as above, is crimes/estimated_population where those numbers come from the Colorado crime statistics "
          HL.a_ [HL.href_ "https://coloradocrimestats.state.co.us/"] "web-site."
    kMeansNotes
  T.writeFile "analysis/moneyBondRateAndCrimeRate.html" $ TL.toStrict $ htmlAsText

moneyBondRateVsCrimeRateVL dataRecords =
  let dat = FV.recordsToVLData (transformF @MoneyBondRate (*100) . transformF @CRate (*100)) dataRecords
      enc = GV.encoding
        . GV.position GV.X [FV.pName @MoneyBondRate, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "Money Bond Rate (%)"]]
        . GV.position GV.Y [FV.pName @CRate, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "Crime Rate (%)"]]
        . GV.color [FV.mName @Year, GV.MmType GV.Nominal]
        . GV.size [FV.mName @EstPopulation, GV.MmType GV.Quantitative, GV.MLegend [GV.LTitle "population"]]
--      transform = GV.transform . GV.filter (GV.FRange "money_bond_rate" (GV.NumberRange 0 100)) 
      sizing = [GV.autosize [GV.AFit], GV.height 400, GV.width 800]
      vl = GV.toVegaLite $
        [ GV.description "Vega-lite"
        , GV.title ("Crime rate vs. Money Bond rate")
        , GV.background "white"
        , GV.mark GV.Point []
        , enc []
        , dat] <> sizing
  in vl

postedBondRateVsCrimeRateVL dataRecords =
  let dat = FV.recordsToVLData (transformF @PostedBondRate (*100) . transformF @CRate (*100)) dataRecords
      enc = GV.encoding
        . GV.position GV.X [FV.pName @PostedBondRate, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "Posted Bond Rate (%)"]]
        . GV.position GV.Y [FV.pName @CRate, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "Crime Rate (%)"]]
        . GV.color [FV.mName @Year, GV.MmType GV.Nominal]
        . GV.size [FV.mName @EstPopulation, GV.MmType GV.Quantitative, GV.MLegend [GV.LTitle "population"]]
--      transform = GV.transform . GV.filter (GV.FRange "money_bond_rate" (GV.NumberRange 0 100)) 
      sizing = [GV.autosize [GV.AFit], GV.height 400, GV.width 800]
      vl = GV.toVegaLite $
        [ GV.description "Vega-lite"
        , GV.title ("Crime rate vs. Posted Bond rate")
        , GV.background "white"
        , GV.mark GV.Point []
        , enc []
        , dat] <> sizing
  in vl

        
-- NB: The data has two rows for each county and year, one for misdemeanors and one for felonies.
--type MoneyPct = "moneyPct" F.:-> Double --F.declareColumn "moneyPct" ''Double
kmMoneyBondPctAnalysis joinedData = do
  putStrLn "Doing money bond % vs poverty rate analysis..."
  htmlAsText <- H.makeReportHtmlAsText "Colorado Money Bonds and Poverty" $ do
      -- this does two things.  Sets the type for rcast to infer and then mutates it.  Those should likely be separate.
    let mutateData :: F.Record [Year,County,OffType,Urbanicity,PovertyR, MoneyBondFreq,TotalBondFreq,TotalPop]
                   -> F.Record [Year,County,OffType,Urbanicity,PovertyR,MoneyBondRate,TotalPop]
        mutateData  = runcurryX (\y c ot u pr mbf tbf tp -> y &: c &: ot &: u &: pr &: (realToFrac mbf)/(realToFrac tbf) &: tp &: V.RNil)
         --countyBondPlusFIPSAndSAIPEAndVera
        kmData = fmap mutateData . catMaybes $ fmap (F.recMaybe . F.rcast) joinedData
        dataProxy = Proxy @[PovertyR,MoneyBondRate,TotalPop]
        -- this rescales both x and y data so they have mean 0, std-dev of 1 and thus k-means will treat both variables as roughly
        -- equivalent when clustering.
        sunXF = FL.premap (F.rgetField @PovertyR &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        sunYF = FL.premap (F.rgetField @MoneyBondRate &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
        kmMoneyBondRatevsPovertyRate proxy_ks = KM.kMeans proxy_ks dataProxy sunXF sunYF 5 KM.partitionCentroids KM.euclidSq        
        (kmByYear, kmByYearUrb) = runIdentity $ FL.foldM ((,)
                                                           <$> kmMoneyBondRatevsPovertyRate (Proxy @[Year,OffType])
                                                           <*> kmMoneyBondRatevsPovertyRate (Proxy @[Year,OffType,Urbanicity])) kmData
    H.placeTextSection $ do
      HL.h2_ "Colorado Money Bond Rate and Poverty (preliminary)"
      HL.p_ [HL.class_ "subtitle"] "Adam Conner-Sax"
      HL.p_ "Colorado issues two types of bonds when releasing people from jail before trial. Sometimes people are released on a \"money bond\" and sometimes on a personal recognizance bond. We have county-level data of all bonds (?) issued in 2014, 2015 and 2016.  Plotting it all is very noisy (since there are 64 counties in CO) so we use a population-weighted k-means clustering technique to combine similar counties into clusters. In the plots below, each circle represents one cluster of counties and the circle's size represents the total population of all the counties included in the cluster.  We consider felonies and misdemeanors separately."
    H.placeVisualization "mBondsVspRateByYrFelonies" $ moneyBondPctVsPovertyRateVL False kmByYear
    H.placeTextSection $ HL.h3_ "Broken down by \"urbanicity\":"
    H.placeVisualization "mBondsVspRateByYrUrbFelonies" $ moneyBondPctVsPovertyRateVL True kmByYearUrb
    H.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ "Denver, the only urban county in CO, didn't report misdemeanors in this data, so the last plot is blank."
        HL.li_ $ do
          HL.span_ "Money Bond Rate is (number of money bonds/total number of bonds). That data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv."
        HL.li_ $ do
          HL.span_ "Poverty Rate comes from the census and we have that data in "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond-SAIPE.csv"] "complete-county-bond-SAIPE.csv"
          HL.span_ ". That data originates from the "
          HL.a_ [HL.href_ "https://www.census.gov/programs-surveys/saipe.html"] "SAIPE website"
          HL.span_ " at the census bureau."
        HL.li_ $ do
          HL.span_ "Urbanicity classification comes from the "
          HL.a_ [HL.href_ "https://www.vera.org/projects/incarceration-trends"] "VERA"
          HL.span_ " data. In the repo "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/county_bond_saipe_vera_data.csv"] "here."
    kMeansNotes
  T.writeFile "analysis/moneyBondRateAndPovertyRate.html" $ TL.toStrict $ htmlAsText

moneyBondPctVsPovertyRateVL facetByUrb dataRecords =
  let dat = FV.recordsToVLData (transformF @MoneyBondRate (*100)) dataRecords
      enc = GV.encoding
        . GV.position GV.X [FV.pName @PovertyR, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "Poverty Rate (%)"]]
        . GV.position GV.Y [FV.pName @MoneyBondRate, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "% Money Bonds"]]
        . GV.color [FV.mName @Year, GV.MmType GV.Nominal]
        . GV.size [FV.mName @TotalPop, GV.MmType GV.Quantitative, GV.MLegend [GV.LTitle "population"]]
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

kMeansNotes =  H.placeTextSection $ do
  HL.h3_ "Some notes on weighted k-means"
  HL.ul_ $ do
    HL.li_ "k-means works by choosing random starting locations for cluster centers, assigning each data-point to the nearest cluster center, moving the center of each cluster to the weighted average of the data-points assigned to the same cluster and then repeating until no data-points move to a new cluster."
    HL.li_ "How do you define distance between points?  Each axis has a different sort of data and it's not clear how to combine differences in each into a meanignful overall distance.  Here, before we hand the data off to the k-means algorithm, we shift and rescale the data so that the set of points has mean 0 and std-deviation 1 in each variable.  Then we use ordinary Euclidean distance, that is the sum of the squares of the differences in each coordinate.  Before plotting, we reverse that scaling so that we can visualize the data in the original. This attempts to give the two variables equal weight in determining what \"nearby\" means for these data points."
    HL.li_ "This clustering happens for each combination plotted."

transformF :: forall x rs. (V.KnownField x, x ∈ rs) => (FA.FType x -> FA.FType x) -> F.Record rs -> F.Record rs
transformF f r = F.rputField @x (f $ F.rgetField @x r) r


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


--  FM.writeCSV_Maybe "data/countyBondPlusFIPS.csv" countyBondPlusFIPS
--  FM.writeCSV_Maybe "data/countyBondPlusSAIPE.csv" countyBondPlusFIPSAndSAIPE
--  FM.writeCSV_Maybe "data/countyBondPlus.csv" countyBondPlusFIPSAndSAIPEAndVera
--  coloradoRowCheck <- F.runSafeEffect $ FL.purely P.fold (goodDataByKey  [F.pr1|Year|]) coloradoTrendsData
--  putStrLn $ "(CO rows, CO rows with all fields) = " ++ show coloradoRowCheck



