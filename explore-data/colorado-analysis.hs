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
import qualified Frames.VegaLiteTemplates   as FV
import qualified Frames.Transform           as FT
import qualified Math.Rescale               as MR
import qualified System.PipesLogger         as SL
import qualified Html.Report                as H


import           Control.Arrow              ((&&&))
import qualified Control.Foldl              as FL
import           Control.Monad.IO.Class     (liftIO)
import           Control.Monad.State (evalStateT, MonadState (..))
import           Control.Lens ((%~),(^.))
import           Data.Bool                  (bool)
import           Data.Functor.Identity      (runIdentity)
import           Data.IORef                 (newIORef)
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
import           Data.Random.Source.PureMT as R
import           Data.Random as R

-- stage restriction means this all has to be up top
F.tableTypes' (F.rowGen fipsByCountyFP) { F.rowTypeName = "FIPSByCountyRenamed", F.columnNames = ["fips","County","State"]}

type CO_AnalysisVERA_Cols = [Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop]
 
justsFromRec :: V.RMap fs => F.Record fs -> F.Rec (Maybe :. F.ElField) fs
justsFromRec = V.rmap (V.Compose . Just)

rDiv :: (Real a, Real b, Fractional c) => a -> b -> c
rDiv a b = realToFrac a/realToFrac b

type instance FI.VectorFor (Maybe a) = V.Vector

main :: IO ()
main = do
  -- create streams which are filtered to CO
  randomSrc <- newIORef (pureMT 1)
  flip (R.runRVarTWith id) randomSrc $ SL.runLoggerIO $ do
    SL.wrapPrefix "Main" $ do
      SL.log SL.Info "Creating data producers from CSV files"
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
      SL.log SL.Info "loading producers into memory for joining"
      fipsByCountyFrame <- liftIO $ F.inCoreAoS $ fipsByCountyData P.>-> P.map (F.rcast @[Fips,County]) -- get rid of state col
      povertyFrame <- liftIO $ F.inCoreAoS $ povertyData P.>-> P.map (F.rcast @[Fips, Year, MedianHI,MedianHIMOE,PovertyR])
      countyBondFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM countyBondCO_Data
      veraFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM $ veraData P.>-> P.map (F.rcast @[Fips,Year,TotalPop,Urbanicity,IndexCrime])
      countyDistrictFrame <- liftIO $F.inCoreAoS countyDistrictCO_Data
      SL.log SL.Info $ (T.pack $ show $ FL.fold FL.length fipsByCountyFrame) <> " rows in fipsByCountyFrame."
      SL.log SL.Info $ (T.pack $ show $ FL.fold FL.length povertyFrame) <> " rows in povertyFrame."
      SL.log SL.Info $ (T.pack $ show $ FL.fold FL.length countyBondFrameM) <> " rows in countyBondFrameM."
      SL.log SL.Info $ (T.pack $ show $ FL.fold FL.length veraFrameM) <> " rows in veraFrameM."
      SL.log SL.Info $ (T.pack $ show $ FL.fold FL.length countyDistrictFrame) <> " rows in countyDistrictFrame."
    -- do joins
      SL.log SL.Info $ "Doing initial the joins..."
      let countyBondPlusFIPS = FM.leftJoinMaybe (Proxy @'[County]) countyBondFrameM (justsFromRec <$> fipsByCountyFrame)
          countyBondPlusFIPSAndDistrict = FM.leftJoinMaybe (Proxy @'[County]) (F.boxedFrame countyBondPlusFIPS) (justsFromRec <$> countyDistrictFrame)
          countyBondPlusFIPSAndSAIPE = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndDistrict) (justsFromRec <$> povertyFrame)
          countyBondPlusFIPSAndSAIPEAndVera = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndSAIPE) veraFrameM      
      kmMoneyBondPctAnalysis countyBondPlusFIPSAndSAIPEAndVera
      bondVsCrimeAnalysis countyBondCO_Data crimeStatsCO_Data
      return ()

-- CrimeRate is defined in DataSources since we use it in more places
type MoneyBondRate = "money_bond_rate" F.:-> Double
type PostedBondRate = "posted_bond_rate" F.:-> Double
type ReoffenseRate = "reoffense_rate" F.:-> Double

moneyBondRate r = let t = r ^. totalBondFreq in FT.recordSingleton @MoneyBondRate $ bool ((r ^. moneyBondFreq) `rDiv` t) 0 (t == 0)  
postedBondRate r = let t = r ^. totalBondFreq in FT.recordSingleton @PostedBondRate $ bool ((r ^. moneyPosted + r ^. prPosted) `rDiv` t) 0 (t == 0)
cRate r = FT.recordSingleton @CrimeRate $ (r ^. crimes) `rDiv` (r ^. estPopulation)
reoffenseRate r = FT.recordSingleton @ReoffenseRate $ (r ^. moneyNewYes + r ^. prNewYes) `rDiv` (r ^. moneyPosted + r ^. prPosted) 

bondVsCrimeAnalysis :: (P.MonadIO m, MonadRandom m)
                    => P.Producer (FM.MaybeRow CountyBondCO) (F.SafeT IO) ()
                    -> P.Producer (FM.MaybeRow CrimeStatsCO) (F.SafeT IO) () -> SL.Logger m ()
bondVsCrimeAnalysis bondDataMaybeProducer crimeDataMaybeProducer = SL.wrapPrefix "BondRateVsCrimeRate" $ do
  SL.log SL.Info "Doing bond rate vs crime rate analysis..."
  countyBondFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeT $ P.toListM bondDataMaybeProducer
  let blanksToZeroes :: F.Rec (Maybe :. F.ElField) '[Crimes,Offenses] -> F.Rec (Maybe :. F.ElField) '[Crimes,Offenses]
      blanksToZeroes = FM.fromMaybeMono 0
  -- turn Nothing into 0, sequence the Maybes out, use concat to "fold" over those maybes, removing any nothings
  crimeStatsList <- liftIO $ F.runSafeT $ P.toListM $ crimeDataMaybeProducer P.>-> P.map (F.recMaybe . (F.rsubset %~ blanksToZeroes)) P.>-> P.concat
  let foldAllCrimes :: FL.Fold (F.Record (F.RecordColumns CrimeStatsCO)) (F.FrameRec [County,Year,Crimes,Offenses,EstPopulation])
      foldAllCrimes = FA.aggregateFs (Proxy @[County,Year]) V.Identity mergeCrimeTypes (0 &: 0 &: 0 &: V.RNil) V.Identity where
        mergeCrimeTypes soFar = FT.bfApply (FT.bfPlus V.:& FT.bfPlus V.:& FT.bfRHS V.:& V.RNil) soFar . F.rcast @[Crimes,Offenses,EstPopulation]  
      mergedCrimeStatsFrame = FL.fold foldAllCrimes crimeStatsList
      unmergedCrimeStatsFrame = F.boxedFrame crimeStatsList
      foldAllBonds :: FL.Fold (FM.MaybeRow CountyBondCO) (F.FrameRec [County,Year,MoneyBondFreq,TotalBondFreq,MoneyPosted,PrPosted,MoneyNewYes,PrNewYes])
      foldAllBonds = FA.aggregateFs (Proxy @[County,Year]) F.recMaybe addBonds (0 &: 0 &: 0 &: 0 &: 0 &: 0 &: V.RNil) V.Identity where
        addBonds = flip $ V.recAdd . F.rcast @[MoneyBondFreq,TotalBondFreq,MoneyPosted,PrPosted,MoneyNewYes,PrNewYes]
      mergedBondDataFrame = FL.fold foldAllBonds countyBondFrameM
      countyBondAndCrimeMerged = F.leftJoin @[County,Year] mergedBondDataFrame mergedCrimeStatsFrame
      countyBondAndCrimeUnmerged = F.leftJoin @[County,Year] mergedBondDataFrame unmergedCrimeStatsFrame
  SL.log SL.Info "Joined crime data and bond data"    
  SL.log SL.Info $ (T.pack $ show $ FL.fold FL.length crimeStatsList) <> " rows in crimeStatsList (unmerged)."
  SL.log SL.Info $ (T.pack $ show $ FL.fold FL.length mergedCrimeStatsFrame) <> " rows in crimeStatsFrame(merged)."
  let sunCrimeRateF = FL.premap (F.rgetField @CrimeRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) (MR.RescaleNone) id
      sunMoneyBondRateF = FL.premap (F.rgetField @MoneyBondRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) (MR.RescaleNone) id
      kmCrimeRateVsMoneyBondRate proxy_ks =
        let dp = Proxy @[MoneyBondRate, CrimeRate, EstPopulation]
        in fmap (KM.clusteredRows dp (F.rgetField @County)) $ KM.kMeansWithClusters proxy_ks dp sunMoneyBondRateF sunCrimeRateF 10 KM.forgyCentroids KM.euclidSq 
      mbrAndCr r = moneyBondRate r F.<+> cRate r
  kmMergedCrimeRateVsMoneyBondRateByYear <- do
    let select = F.rcast @[Year,County,MoneyBondFreq,TotalBondFreq,Crimes,Offenses,EstPopulation]
        kmData = fmap (FT.mutate mbrAndCr) . catMaybes $ fmap (F.recMaybe . select) countyBondAndCrimeMerged
    SL.log SL.Info "Doing weighted-KMeans on crime rate vs. money-bond rate (merged crime types)."    
    FL.foldM (kmCrimeRateVsMoneyBondRate (Proxy @'[Year])) kmData       
  kmCrimeRateVsMoneyBondRateByYearAndType <- do
    let select = F.rcast @[Year,County,CrimeAgainst,MoneyBondFreq,TotalBondFreq,Crimes,Offenses,EstPopulation]
        kmData = fmap (FT.mutate mbrAndCr) . catMaybes $ fmap (F.recMaybe . select) countyBondAndCrimeUnmerged
    SL.log SL.Info "Doing weighted-KMeans on crime rate vs. money-bond rate (separate crime types)."        
    FL.foldM (kmCrimeRateVsMoneyBondRate (Proxy @[Year,CrimeAgainst])) kmData 
  let sunPostedBondRateF = FL.premap (F.rgetField @PostedBondRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id      
      pbrAndCr r = postedBondRate r F.<+> cRate r
      kmCrimeRateVsPostedBondRate proxy_ks =
        let dp = Proxy @[PostedBondRate, CrimeRate, EstPopulation]
        in fmap (KM.clusteredRows dp (F.rgetField @County)) $ KM.kMeansWithClusters proxy_ks dp sunPostedBondRateF sunCrimeRateF 10 KM.forgyCentroids KM.euclidSq 
--        in KM.kMeans proxy_ks dp sunPostedBondRateF sunCrimeRateF 10 (\x y -> return $ KM.partitionCentroids x y) KM.euclidSq       
  kmMergedCrimeRateVsPostedBondRateByYear <- do
    let select = F.rcast @[Year,County,TotalBondFreq,MoneyPosted,PrPosted,Crimes,Offenses,EstPopulation]
        kmData = fmap (FT.mutate pbrAndCr) . catMaybes $ fmap (F.recMaybe . select) countyBondAndCrimeMerged
    SL.log SL.Info "Doing weighted-KMeans on crime rate vs. posted-bond rate (merged)."    
    FL.foldM (kmCrimeRateVsPostedBondRate (Proxy @'[Year])) kmData 
        
  kmCrimeRateVsPostedBondRateByYearAndType <- do      
    let select = F.rcast @[Year,County,CrimeAgainst,TotalBondFreq,MoneyPosted,PrPosted,Crimes,Offenses,EstPopulation]
        kmData = fmap (FT.mutate pbrAndCr) . catMaybes $ fmap (F.recMaybe . select) countyBondAndCrimeUnmerged
    SL.log SL.Info "Doing weighted-KMeans on crime rate vs. posted-bond rate (unmerged)."            
    FL.foldM (kmCrimeRateVsPostedBondRate (Proxy @[Year,CrimeAgainst])) kmData
    
  let sunReoffenseRateF = FL.premap (F.rgetField @ReoffenseRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
      rorAndMbr r = reoffenseRate r F.<+> moneyBondRate r 
      kmReoffenseRatevsMoneyBondRate proxy_ks =
        let dp = Proxy @[MoneyBondRate, ReoffenseRate, EstPopulation]
        in fmap (KM.clusteredRows dp (F.rgetField @County)) $ KM.kMeansWithClusters proxy_ks dp sunMoneyBondRateF sunReoffenseRateF 10 KM.forgyCentroids KM.euclidSq 
--        in KM.kMeans proxy_ks dp sunMoneyBondRateF sunReoffenseRateF 10 (\x y -> return $ KM.partitionCentroids x y) KM.euclidSq       
  kmReoffenseRateVsMergedMoneyBondRateByYear <- do
    let select = F.rcast @[Year, County, MoneyNewYes, PrNewYes, MoneyPosted, PrPosted, TotalBondFreq, MoneyBondFreq, EstPopulation]
        kmData = fmap (FT.mutate rorAndMbr) . catMaybes $ fmap (F.recMaybe . select) countyBondAndCrimeMerged
    SL.log SL.Info "Doing weighted-KMeans on re-offense rate vs. money-bond rate (merged)."            
    FL.foldM (kmReoffenseRatevsMoneyBondRate (Proxy @'[Year])) kmData 

  SL.log SL.Info "Creating Html"
  htmlAsText <- H.makeReportHtmlAsText "Colorado Money Bond Rate vs Crime rate" $ do
    H.placeTextSection $ do
      HL.h2_ "Colorado Bond Rates and Crime Rates (preliminary)"
      HL.p_ [HL.class_ "subtitle"] "Adam Conner-Sax"
      HL.p_ "Each county in Colorado issues money bonds and personal recognizance bonds.  For each county I look at the % of money bonds out of all bonds issued and the crime rate.  We have 3 years of data and there are 64 counties in Colorado (each with vastly different populations).  So I've used a population-weighted k-means clustering technique to reduce the number of points to at most 7 per year. Each circle in the plot below represents one cluster of counties with similar money bond and poverty rates.  The size of the circle represents the total population in the cluster."
    H.placeVisualization "crimeRateVsMoneyBondRateMerged" $ cRVsMBRVL True kmMergedCrimeRateVsMoneyBondRateByYear    
    HL.p_ "Broken down by Colorado's crime categories:"
    H.placeVisualization "crimeRateVsMoneyBondRateUnMerged" $ cRVsMBRVL False kmCrimeRateVsMoneyBondRateByYearAndType
    H.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ $ do
          HL.span_ "Money Bond Rate is (number of money bonds/total number of bonds). That data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv"
          HL.span_ ". NB: Bond rates are not broken down by crime type because they are not broken down in the data we have."
        HL.li_ $ do
          HL.span_ "Crime Rate is crimes/estimated_population. Those numbers come from the Colorado crime statistics "
          HL.a_ [HL.href_ "https://coloradocrimestats.state.co.us/"] "web-site"
          HL.span_ ".  We also have some of that data in the "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/crime-rate-bycounty.csv"] "repo"
          HL.span_ ", but only for 2016, so I re-downloaded it for this. Also, there are some crimes in that data that don't roll up to a particular county but instead are attributed to the Colorado Bureau of Investigation or the Colorado State Patrol.  I've ignored those."
    H.placeTextSection $ do
      HL.p_ "Below I look at the percentage of all bonds which are \"posted\" (posting bond means paying the money bond or agreeing to the terms of the personal recognizance bond ?) rather than the % of money bonds. I use the same clustering technique."
    H.placeVisualization "crimeRateVsPostedBondRate" $ cRVsPBRVL True kmMergedCrimeRateVsPostedBondRateByYear
    HL.p_ "Broken down by Colorado's crime categories:"
    H.placeVisualization "crimeRateVsPostedBondRateUnMerged" $ cRVsPBRVL False kmCrimeRateVsPostedBondRateByYearAndType
    H.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ $ do
          HL.span_ "Posted Bond Rate is [(number of posted money bonds + number of posted PR bonds)/total number of bonds] where that data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv."
        HL.li_ $ do
          HL.span_ "Crime Rate, as above, is crimes/estimated_population where those numbers come from the Colorado crime statistics "
          HL.a_ [HL.href_ "https://coloradocrimestats.state.co.us/"] "web-site."
    HL.p_ "Below I look at the rate of re-offending among people out on bond, here considered in each county along with money-bond rate and clustered as above."       
    H.placeVisualization "reoffenseRateVsMoneyBondRate" $ rRVsMBRVL True kmReoffenseRateVsMergedMoneyBondRateByYear
    H.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ $ do
          HL.span_ "Reoffense Rate is (number of new offenses for all bonds/total number of posted bonds) where that data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv."
    
    kMeansNotes
  liftIO $ T.writeFile "analysis/moneyBondRateAndCrimeRate.html" $ TL.toStrict $ htmlAsText

cRVsMBRVL mergedOffenseAgainst dataRecords =
  let dat = FV.recordsToVLData (transformF @MoneyBondRate (*100) . transformF @CrimeRate (*100)) dataRecords
      ptEnc = GV.color [FV.mName @Year, GV.MmType GV.Nominal]
              . GV.size [FV.mName @EstPopulation, GV.MmType GV.Quantitative, GV.MLegend [GV.LTitle "population"]]
  in case mergedOffenseAgainst of
    True ->
      let vlF = FV.clustersWithClickIntoVL @MoneyBondRate @CrimeRate @KM.IsCentroid @KM.ClusterId @KM.MarkLabel @'[Year]         
      in vlF "Money Bond Rate (%, All Crimes)" "Crime Rate (%, All Crimes)" "Crime Rate vs Money Bond Rate" ptEnc id id id dat
    False ->
      let ptEncFaceted = ptEnc
                         . GV.row [FV.fName @CrimeAgainst, GV.FmType GV.Nominal,GV.FHeader [GV.HTitle "Crime Against"]]
          vlF = FV.clustersWithClickIntoVL @MoneyBondRate @CrimeRate @KM.IsCentroid @KM.ClusterId @KM.MarkLabel @[Year,CrimeAgainst]
      in vlF "Money Bond Rate (%, All Crimes)" "Crime Rate (%)" "Crime Rate vs Money Bond Rate" ptEncFaceted id id id dat

cRVsPBRVL mergedOffenseAgainst dataRecords =
  let dat = FV.recordsToVLData (transformF @PostedBondRate (*100) . transformF @CrimeRate (*100)) dataRecords
      ptEnc = GV.color [FV.mName @Year, GV.MmType GV.Nominal]
              . GV.size [FV.mName @EstPopulation, GV.MmType GV.Quantitative, GV.MLegend [GV.LTitle "population"]]
  in case mergedOffenseAgainst of
    True ->
      let vlF = FV.clustersWithClickIntoVL @PostedBondRate @CrimeRate @KM.IsCentroid @KM.ClusterId @KM.MarkLabel @'[Year]         
      in vlF "Posted Bond Rate (%, All Crimes)" "Crime Rate (%, All Crimes)" "Crime Rate vs Posted Bond Rate" ptEnc id id id dat
    False ->
      let ptEncFaceted = ptEnc
                         . GV.row [FV.fName @CrimeAgainst, GV.FmType GV.Nominal,GV.FHeader [GV.HTitle "Crime Against"]]
          vlF = FV.clustersWithClickIntoVL @PostedBondRate @CrimeRate @KM.IsCentroid @KM.ClusterId @KM.MarkLabel @[Year,CrimeAgainst]
      in vlF "Posted Bond Rate (%, All Crimes)" "Crime Rate (%)" "Crime Rate vs Posted Bond Rate" ptEncFaceted id id id dat

rRVsMBRVL mergedOffenseAgainst dataRecords =
  let dat = FV.recordsToVLData (transformF @MoneyBondRate (*100) . transformF @ReoffenseRate (*100)) dataRecords
      ptEnc = GV.color [FV.mName @Year, GV.MmType GV.Nominal]
              . GV.size [FV.mName @EstPopulation, GV.MmType GV.Quantitative, GV.MLegend [GV.LTitle "population"]]
  in case mergedOffenseAgainst of
    True ->
      let vlF = FV.clustersWithClickIntoVL @MoneyBondRate @ReoffenseRate @KM.IsCentroid @KM.ClusterId @KM.MarkLabel @'[Year]         
      in vlF "Money Bond Rate (%, All Crimes)" "Reoffense Rate (%, All Crimes)" "Reoffense Rate vs Money Bond Rate" ptEnc id id id dat
    False ->
      let ptEncFaceted = ptEnc
                         . GV.row [FV.fName @CrimeAgainst, GV.FmType GV.Nominal,GV.FHeader [GV.HTitle "Crime Against"]]
          vlF = FV.clustersWithClickIntoVL @MoneyBondRate @CrimeRate @KM.IsCentroid @KM.ClusterId @KM.MarkLabel @[Year,CrimeAgainst]
      in vlF "Money Bond Rate (%, All Crimes)" "Reoffense Rate (%)" "Reoffense Rate vs Money Bond Rate" ptEncFaceted id id id dat

        
-- NB: The data has two rows for each county and year, one for misdemeanors and one for felonies.
--type MoneyPct = "moneyPct" F.:-> Double --F.declareColumn "moneyPct" ''Double
kmMoneyBondPctAnalysis joinedData = SL.wrapPrefix "MoneyBondVsPoverty" $ do
  SL.log SL.Info "Doing money bond % vs poverty rate analysis..."
  let select = F.rcast @[Year,County,OffType,Urbanicity,PovertyR, MoneyBondFreq,TotalBondFreq,TotalPop]
      mutation r = moneyBondRate r 
      kmData = fmap (FT.mutate mutation) . catMaybes $ fmap (F.recMaybe . select) joinedData
      dataProxy = Proxy @[PovertyR,MoneyBondRate,TotalPop]
      sunPovertyRF = FL.premap (F.rgetField @PovertyR) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
      sunMoneyBondRateF = FL.premap (F.rgetField @MoneyBondRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
      kmMoneyBondRatevsPovertyRate proxy_ks =
        KM.kMeans proxy_ks dataProxy sunPovertyRF sunMoneyBondRateF 5 (\x y -> return $ KM.partitionCentroids x y) KM.euclidSq        
  (kmByYear, kmByYearUrb) <- FL.foldM ((,)
                                        <$> kmMoneyBondRatevsPovertyRate (Proxy @[Year,OffType])
                                        <*> kmMoneyBondRatevsPovertyRate (Proxy @[Year,OffType,Urbanicity])) kmData
  SL.log SL.Info "Creating Html"
  htmlAsText <- H.makeReportHtmlAsText "Colorado Money Bonds and Poverty" $ do    
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
  liftIO $ T.writeFile "analysis/moneyBondRateAndPovertyRate.html" $ TL.toStrict $ htmlAsText

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
indexCrimeRate r = FT.recordSingleton @IndexCrimeRate $ (r ^. indexCrime) `rDiv` (r ^. totalPop)
totalBondRate r = FT.recordSingleton @TotalBondRate $ (r ^. totalBondFreq) `rDiv` (r ^. totalPop) 
kmBondRatevsCrimeRateAnalysis joinedData = do
  let select = F.rcast @[Year,County,Urbanicity,TotalBondFreq,IndexCrime,TotalPop]
      mutation r = totalBondRate r F.<+> indexCrimeRate r
      mData = fmap (FT.mutate mutation) . catMaybes $ fmap (F.recMaybe . select) joinedData
  F.writeCSV "data/raw_COCrimeRatevsBondRate.csv" mData
{-  let dataCols = Proxy @[TotalBondRate, IndexCrimeRate, TotalPop]
      xScaling = FL.premap (F.rgetField @TotalBondRate &&& F.rgetField @TotalPop) $ MR.weightedScaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
      yScaling = FL.premap (F.rgetField @IndexCrimeRate &&& F.rgetField @TotalPop) $ MR.weightedScaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
-}


--  FM.writeCSV_Maybe "data/countyBondPlusFIPS.csv" countyBondPlusFIPS
--  FM.writeCSV_Maybe "data/countyBondPlusSAIPE.csv" countyBondPlusFIPSAndSAIPE
--  FM.writeCSV_Maybe "data/countyBondPlus.csv" countyBondPlusFIPSAndSAIPEAndVera
--  coloradoRowCheck <- F.runSafeEffect $ FL.purely P.fold (goodDataByKey  [F.pr1|Year|]) coloradoTrendsData
--  putStrLn $ "(CO rows, CO rows with all fields) = " ++ show coloradoRowCheck



