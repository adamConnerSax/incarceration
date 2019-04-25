{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE PartialTypeSignatures     #-}
--{-# LANGUAGE MonomorphismRestriction #-}
module Main where

import           DataSources
import qualified Frames.Utils               as FU
import qualified Frames.Folds               as FF
import qualified Frames.KMeans              as KM
import qualified Frames.Regression          as FR
import qualified Frames.MaybeUtils          as FM
import qualified Frames.VegaLite            as FV
import qualified Frames.Transform           as FT
import qualified Frames.Table               as Table
import qualified Math.Rescale               as MR
import qualified Frames.MapReduce           as MR

import qualified Knit.Report                as K
import qualified Knit.Effect.RandomFu       as KR
import qualified Knit.Effect.Pandoc         as K (newPandoc, NamedDoc (..))
import qualified Knit.Report.Other.Lucid    as KL
{-
import qualified Polysemy             as PS
import qualified Knit.Effects.Logger      as Log
import qualified Knit.Effects.PandocMonad as PM
import qualified Knit.Effects.Pandoc      as PE
import           Knit.Effects.RandomFu      (Random, runRandomIOPureMT)
import           Knit.Effects.Docs        (toNamedDocListWithM)
--import qualified Knit.Report.Blaze            as RB
import qualified Knit.Report.Pandoc              as RP
import qualified Knit.Report.Lucid            as RL
-}

import qualified Control.Foldl              as FL
import           Control.Monad.IO.Class     (MonadIO, liftIO)
import           Control.Monad              (sequence)
import           Control.Lens ((%~),(^.))
import           Data.Bool                  (bool)
import qualified Data.Map                   as M
import           Data.Maybe                 (catMaybes, fromJust, fromMaybe)
import           Data.Monoid                ((<>))
import qualified Data.Monoid                as MO
import           Data.Proxy                 (Proxy (..))
import qualified Data.Text                  as T
import qualified Data.Text.IO               as T
import qualified Data.Text.Lazy             as TL
import qualified Data.Vector                as V
import qualified Data.Vinyl                 as V
import qualified Data.Vinyl.Functor         as V
import qualified Data.Vinyl.Class.Method    as V
import qualified Data.Vinyl.TypeLevel       as V
import           Data.Vinyl.Lens            (type (∈))
import           Frames                     ((:.), (&:))
import qualified Frames                     as F
import qualified Frames.CSV                 as F
import qualified Frames.Melt                 as F
import qualified Frames.InCore              as FI
import qualified Frames.TH                  as F
import qualified Graphics.Vega.VegaLite     as GV
import qualified Pipes                      as P
import qualified Pipes.Prelude              as P
import qualified Lucid                      as HL
import qualified Text.Blaze.Html.Renderer.Text as BH
import           Data.Random.Source.PureMT as R
import           Data.Random as R
import qualified System.Clock as C
import qualified Statistics.Types as ST
-- stage restriction means this all has to be up top
F.tableTypes' (F.rowGen fipsByCountyFP) { F.rowTypeName = "FIPSByCountyRenamed", F.columnNames = ["fips","County","State"]}

type CO_AnalysisVERA_Cols = [Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop]
 
justsFromRec :: V.RMap fs => F.Record fs -> F.Rec (Maybe :. F.ElField) fs
justsFromRec = V.rmap (V.Compose . Just)

rDiv :: (Real a, Real b, Fractional c) => a -> b -> c
rDiv a b = realToFrac a/realToFrac b

type instance FI.VectorFor (Maybe a) = V.Vector

type DblX = "X" F.:-> Double
type DblY = "Y" F.:-> Double

templateVars = M.fromList
  [
    ("lang", "English")
  , ("author", "Adam Conner-Sax")
  , ("pagetitle", "Colorado Incarceration Data Analysis")
  , ("tufte","True")
  ]

main :: IO ()
main = do
  -- create streams which are filtered to CO
  let writeNamedHtml (K.NamedDoc n lt) = T.writeFile (T.unpack $ "analysis/" <> n <> ".html") $ TL.toStrict lt
      writeAllHtml = fmap (const ()) . traverse writeNamedHtml
      pandocWriterConfig = K.PandocWriterConfig (Just "pandoc-templates/minWithVega-pandoc.html")  templateVars K.mindocOptionsF
  startReal <- C.getTime C.Monotonic
  eitherDocs <- K.knitHtmls (Just "colorado-analysis.Main") K.logAll pandocWriterConfig $ KR.runRandomIOPureMT (pureMT 1) $ do
    K.logLE K.Info "Creating data producers from CSV files"
    let parserOptions = F.defaultParser { F.quotingMode =  F.RFC4180Quoting ' ' }
        veraData :: F.MonadSafe m => P.Producer (FM.MaybeRow IncarcerationTrends)  m ()
        veraData = F.readTableMaybeOpt F.defaultParser veraTrendsFP  P.>-> P.filter (FU.filterOnMaybeField @State (=="CO"))
        povertyData :: F.MonadSafe m => P.Producer SAIPE m ()
        povertyData = F.readTableOpt parserOptions censusSAIPE_FP P.>-> P.filter (FU.filterOnField @Abbreviation (== "CO"))
        fipsByCountyData :: F.MonadSafe m => P.Producer FIPSByCountyRenamed m ()
        fipsByCountyData = F.readTableOpt parserOptions fipsByCountyFP  P.>-> P.filter (FU.filterOnField @State (== "CO"))
        -- NB: This data has 2 rows per county, one for misdemeanors, one for felonies
        countyBondCO_Data :: F.MonadSafe m => P.Producer (FM.MaybeRow CountyBondCO) m ()
        countyBondCO_Data = F.readTableMaybeOpt parserOptions countyBondCO_FP
        countyDistrictCO_Data :: F.MonadSafe m => P.Producer CountyDistrictCO m ()
        countyDistrictCO_Data = F.readTableOpt parserOptions countyDistrictCrosswalkCO_FP
        -- This data has 3 rows per county and year, one for each type of crime (against persons, against property, against society)
        crimeStatsCO_Data :: F.MonadSafe m => P.Producer (FM.MaybeRow CrimeStatsCO) m ()
        crimeStatsCO_Data = F.readTableMaybeOpt parserOptions crimeStatsCO_FP
    -- load streams into memory for joins, subsetting as we go
    K.logLE K.Info "loading producers into memory for joining"
    fipsByCountyFrame <- liftIO $ F.inCoreAoS $ fipsByCountyData P.>-> P.map (F.rcast @[Fips,County]) -- get rid of state col
    povertyFrame <- liftIO $ F.inCoreAoS $ povertyData P.>-> P.map (F.rcast @[Fips, Year, MedianHI,MedianHIMOE,PovertyR])
    countyBondFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM countyBondCO_Data
    veraFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeEffect $ P.toListM $ veraData P.>-> P.map (F.rcast @[Fips,Year,TotalPop,Urbanicity,IndexCrime])
    countyDistrictFrame <- liftIO $F.inCoreAoS countyDistrictCO_Data
    K.logLE K.Diagnostic $ (T.pack $ show $ FL.fold FL.length fipsByCountyFrame) <> " rows in fipsByCountyFrame."
    K.logLE K.Diagnostic $ (T.pack $ show $ FL.fold FL.length povertyFrame) <> " rows in povertyFrame."
    K.logLE K.Diagnostic $ (T.pack $ show $ FL.fold FL.length countyBondFrameM) <> " rows in countyBondFrameM."
    K.logLE K.Diagnostic $ (T.pack $ show $ FL.fold FL.length veraFrameM) <> " rows in veraFrameM."
    K.logLE K.Diagnostic $ (T.pack $ show $ FL.fold FL.length countyDistrictFrame) <> " rows in countyDistrictFrame."
    -- do joins
    K.logLE K.Info $ "Doing initial the joins..."
    let countyBondPlusFIPS = FM.leftJoinMaybe (Proxy @'[County]) countyBondFrameM (justsFromRec <$> fipsByCountyFrame)
        countyBondPlusFIPSAndDistrict = FM.leftJoinMaybe (Proxy @'[County]) (F.boxedFrame countyBondPlusFIPS) (justsFromRec <$> countyDistrictFrame)
        countyBondPlusFIPSAndSAIPE = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndDistrict) (justsFromRec <$> povertyFrame)
        countyBondPlusFIPSAndSAIPEAndVera = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndSAIPE) veraFrameM      
    K.newPandoc "moneyBondRateAndPovertyRate" $ kmMoneyBondPctAnalysis countyBondPlusFIPSAndSAIPEAndVera
    K.newPandoc "moneyBondRateAndCrimeRate" $ bondVsCrimeAnalysis countyBondCO_Data crimeStatsCO_Data
    endReal <- liftIO $ C.getTime C.Monotonic
    let realTime = C.diffTimeSpec endReal startReal
        printTime (C.TimeSpec s ns) = (T.pack $ show $ realToFrac s + realToFrac ns/(10^9)) 
    K.logLE K.Info $ "Time (real): " <> printTime realTime <> "s" 
    return ()
  case eitherDocs of
    Right namedDocs -> writeAllHtml namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err

-- extra columns we will need
-- CrimeRate is defined in DataSources since we use it in more places
type MoneyBondRate = "money_bond_rate" F.:-> Double
type PostedBondRate = "posted_bond_rate" F.:-> Double
type PostedBondPerCapita = "posted_bond_per_capita" F.:-> Double
type ReoffenseRate = "reoffense_rate" F.:-> Double
type PostedBondFreq = "posted_bond_freq" F.:-> Int
type CrimeRateError = "crime_rate_error" F.:-> Double
type CrimeRateFitted = "crime_rate_fitted" F.:-> Double
type CrimeRateFittedErr = "crime_rate_fitted_err" F.:-> Double


-- functions to populate some of those columns from other columns
moneyBondRate r = let t = r ^. totalBondFreq in FT.recordSingleton @MoneyBondRate $ bool ((r ^. moneyBondFreq) `rDiv` t) 0 (t == 0)  
postedBondRate r = let t = r ^. totalBondFreq in FT.recordSingleton @PostedBondRate $ bool ((r ^. moneyPosted + r ^. prPosted) `rDiv` t) 0 (t == 0)
postedBondPerCapita r = FT.recordSingleton @PostedBondPerCapita $ (r ^. moneyPosted + r ^. prPosted) `rDiv` (r ^. estPopulation)
cRate r = FT.recordSingleton @CrimeRate $ (r ^. crimes) `rDiv` (r ^. estPopulation)
reoffenseRate r = FT.recordSingleton @ReoffenseRate $ (r ^. moneyNewYes + r ^. prNewYes) `rDiv` (r ^. moneyPosted + r ^. prPosted) 
postedBondFreq r = FT.recordSingleton @PostedBondFreq $ (r ^. moneyPosted + r ^. prPosted)


bondVsCrimeAnalysis :: forall effs. ( MonadIO (K.Semantic effs)
                                    , K.PandocEffects effs
                                    , K.Member K.ToPandoc effs
                                    , K.Member KR.Random effs)
                    => P.Producer (FM.MaybeRow CountyBondCO) (F.SafeT IO) ()
                    -> P.Producer (FM.MaybeRow CrimeStatsCO) (F.SafeT IO) ()
                    -> K.Semantic effs ()
bondVsCrimeAnalysis bondDataMaybeProducer crimeDataMaybeProducer = K.wrapPrefix "BondRateVsCrimeRate" $ do
  K.logLE K.Info "Doing bond rate vs crime rate analysis..."
  countyBondFrameM <- liftIO $ fmap F.boxedFrame $ F.runSafeT $ P.toListM bondDataMaybeProducer
  let blanksToZeroes :: F.Rec (Maybe :. F.ElField) '[Crimes,Offenses] -> F.Rec (Maybe :. F.ElField) '[Crimes,Offenses]
      blanksToZeroes = FM.fromMaybeMono 0
  -- turn Nothing into 0, sequence the Maybes out, use concat to "fold" over those maybes, removing any nothings
  crimeStatsList <- liftIO $ F.runSafeT $ P.toListM $ crimeDataMaybeProducer P.>-> P.map (F.recMaybe . (F.rsubset %~ blanksToZeroes)) P.>-> P.concat
  let sumCrimesFold = FF.sequenceEndoFolds $ FF.FoldEndo FL.sum V.:& FF.FoldEndo FL.sum V.:& FF.FoldEndo (fmap (fromMaybe 0) FL.last) V.:& V.RNil
      foldAllCrimes = MR.concatFold $ MR.hashableMapReduceFold MR.noUnpack (MR.assignKeysAndData @[County, Year] @[Crimes, Offenses, EstPopulation]) (MR.foldAndAddKey sumCrimesFold)
      mergedCrimeStatsFrame = FL.fold foldAllCrimes crimeStatsList
      unmergedCrimeStatsFrame = F.boxedFrame crimeStatsList
      foldAllBonds = MR.concatFold $ MR.hashableMapReduceFold
        (MR.unpackGoodRows @[County,Year,MoneyBondFreq,PrBondFreq,TotalBondFreq,MoneyPosted,PrPosted,MoneyNewYes,PrNewYes])
        (MR.splitOnKeys @[County,Year])
        (MR.foldAndAddKey $ FF.foldAllMonoid @MO.Sum)
      mergedBondDataFrame = FL.fold foldAllBonds countyBondFrameM
      countyBondAndCrimeMerged = F.leftJoin @[County,Year] mergedBondDataFrame mergedCrimeStatsFrame
      countyBondAndCrimeUnmerged = F.leftJoin @[County,Year] mergedBondDataFrame unmergedCrimeStatsFrame
  K.logLE K.Info "Joined crime data and bond data"    
  K.logLE K.Diagnostic $ (T.pack $ show $ FL.fold FL.length crimeStatsList) <> " rows in crimeStatsList (unmerged)."
  K.logLE K.Diagnostic $ (T.pack $ show $ FL.fold FL.length mergedCrimeStatsFrame) <> " rows in crimeStatsFrame(merged)."
  let initialCentroidsF n = MR.functionToFoldM (KM.kMeansPPCentroids @DblX @DblY @EstPopulation KM.euclidSq n) 
      kmReduce f k rows = sequence $ M.singleton k $ f 10 10 initialCentroidsF (KM.weighted2DRecord @DblX @DblY @EstPopulation) KM.euclidSq rows
      sunCrimeRateF = FL.premap (F.rgetField @CrimeRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) (MR.RescaleNone) id
      sunMoneyBondRateF = FL.premap (F.rgetField @MoneyBondRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) (MR.RescaleNone) id
      sunPostedBondRateF = FL.premap (F.rgetField @PostedBondRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id      
      sunReoffenseRateF = FL.premap (F.rgetField @ReoffenseRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
      mbrAndCr r = moneyBondRate r F.<+> cRate r  
      pbrAndCr r = postedBondRate r F.<+> cRate r
      rorAndMbr r = reoffenseRate r F.<+> moneyBondRate r
  kmMergedCrimeRateVsMoneyBondRateByYear  <- do
    K.logLE K.Info "Doing weighted-KMeans on crime rate vs. money-bond rate (merged crime types)."
    let unpack = fmap (FT.mutate mbrAndCr) (MR.unpackGoodRows @[Year,County,MoneyBondFreq,TotalBondFreq,Crimes,Offenses,EstPopulation])
        reduce :: _
        reduce = MR.ReduceM $ kmReduce (KM.kMeansOneWithClusters @MoneyBondRate @CrimeRate @EstPopulation sunMoneyBondRateF sunCrimeRateF)
        kmFoldM = MR.concatFoldM $ MR.hashableMapReduceFoldM (MR.generalizeUnpack unpack) (MR.generalizeAssign $ MR.assignKeys @'[Year]) reduce
    flip FL.foldM countyBondAndCrimeMerged $
      fmap (KM.clusteredRows @MoneyBondRate @CrimeRate @EstPopulation (F.rgetField @County)) $ kmFoldM
  kmCrimeRateVsMoneyBondRateByYearAndType <- do
    K.logLE K.Info "Doing weighted-KMeans on crime rate vs. money-bond rate (separate crime types)."
    let unpack = fmap (FT.mutate mbrAndCr) (MR.unpackGoodRows @[Year,County,CrimeAgainst,MoneyBondFreq,TotalBondFreq,Crimes,Offenses,EstPopulation])
        reduce :: _
        reduce =  MR.ReduceM $ kmReduce (KM.kMeansOneWithClusters @MoneyBondRate @CrimeRate @EstPopulation sunMoneyBondRateF sunCrimeRateF)
        kmFoldM = MR.concatFoldM $ MR.hashableMapReduceFoldM (MR.generalizeUnpack unpack) (MR.generalizeAssign $ MR.assignKeys @'[Year, CrimeAgainst]) reduce   
    flip FL.foldM countyBondAndCrimeUnmerged $
      fmap (KM.clusteredRows @MoneyBondRate @CrimeRate @EstPopulation (F.rgetField @County)) $ kmFoldM
  kmMergedCrimeRateVsPostedBondRateByYear <- do
    K.logLE K.Info "Doing weighted-KMeans on crime rate vs. posted-bond rate (merged)."
    let unpack = fmap (FT.mutate pbrAndCr) (MR.unpackGoodRows @[Year,County,TotalBondFreq,MoneyPosted,PrPosted,Crimes,Offenses,EstPopulation])
        reduce :: _ 
        reduce =  MR.ReduceM $ kmReduce (KM.kMeansOneWithClusters @PostedBondRate @CrimeRate @EstPopulation sunPostedBondRateF sunCrimeRateF)
        kmFoldM = MR.concatFoldM $ MR.hashableMapReduceFoldM (MR.generalizeUnpack unpack) (MR.generalizeAssign $ MR.assignKeys @'[Year]) reduce   
    flip FL.foldM countyBondAndCrimeMerged $
      fmap (KM.clusteredRows @PostedBondRate @CrimeRate @EstPopulation (F.rgetField @County)) $ kmFoldM
  kmCrimeRateVsPostedBondRateByYearAndType <- do      
    K.logLE K.Info "Doing weighted-KMeans on crime rate vs. posted-bond rate (unmerged)."            
    let unpack = fmap (FT.mutate pbrAndCr) (MR.unpackGoodRows @[Year,County,CrimeAgainst,TotalBondFreq,MoneyPosted,PrPosted,Crimes,Offenses,EstPopulation])
        reduce :: _
        reduce = MR.ReduceM $ kmReduce (KM.kMeansOneWithClusters @PostedBondRate @CrimeRate @EstPopulation sunPostedBondRateF sunCrimeRateF)
        kmFoldM = MR.concatFoldM $ MR.hashableMapReduceFoldM (MR.generalizeUnpack unpack) (MR.generalizeAssign $ MR.assignKeys @'[Year,CrimeAgainst]) reduce   
    flip FL.foldM countyBondAndCrimeUnmerged $
      fmap (KM.clusteredRows @PostedBondRate @CrimeRate @EstPopulation (F.rgetField @County)) $ kmFoldM
  kmReoffenseRateVsMergedMoneyBondRateByYear <- do
    K.logLE K.Info "Doing weighted-KMeans on re-offense rate vs. money-bond rate (merged)."            
    let unpack = fmap (FT.mutate rorAndMbr) (MR.unpackGoodRows @[Year, County, MoneyNewYes, PrNewYes, MoneyPosted, PrPosted, TotalBondFreq, MoneyBondFreq, EstPopulation])
        reduce :: _
        reduce = MR.ReduceM $ kmReduce (KM.kMeansOneWithClusters @MoneyBondRate @ReoffenseRate @EstPopulation sunMoneyBondRateF sunReoffenseRateF)
        kmFoldM = MR.concatFoldM $ MR.hashableMapReduceFoldM (MR.generalizeUnpack unpack) (MR.generalizeAssign $ MR.assignKeys @'[Year]) reduce             
    flip FL.foldM countyBondAndCrimeMerged $
      fmap (KM.clusteredRows @MoneyBondRate @ReoffenseRate @EstPopulation (F.rgetField @County)) $ kmFoldM
  -- regressions
  let rMut r = mbrAndCr r F.<+> postedBondFreq r F.<+> postedBondRate r F.<+> postedBondPerCapita r
  (rData, regressionRes, regressionResMany) <- do
    let regUnpack = fmap (FT.mutate rMut) $ (MR.unpackGoodRows @[Year,MoneyBondFreq,PrBondFreq,PrPosted,MoneyPosted,TotalBondFreq,Crimes,EstPopulation])
--        regReduce :: Foldable f => (forall g. Foldable g => g (F.Record rs) -> PS.Semantic effs b) -> k -> f (F.Record rs) -> PS.Semantic effs (M.Map k b)

        regMR r = MR.concatFoldM $ MR.hashableMapReduceFoldM (MR.generalizeUnpack regUnpack) (MR.generalizeAssign $ MR.assignKeys @'[Year]) r --(MR.ReduceM $ regReduce r)
        dataMR = MR.unpackOnlyFoldM (MR.generalizeUnpack regUnpack)
        guess = [0,0] -- guess has one extra dimension for constant
        regressOneBM = MR.functionToFoldM $ return . FR.leastSquaresByMinimization @Crimes @'[EstPopulation, MoneyBondFreq] False guess
        regressOneOLS = MR.functionToFoldM $ FR.ordinaryLeastSquares @effs @Crimes @False @'[EstPopulation]
        regressOneWLS = MR.functionToFoldM $ FR.popWeightedLeastSquares @effs @CrimeRate @True @'[] @EstPopulation 
        regressOneWLS2 = MR.functionToFoldM $ FR.varWeightedLeastSquares @effs @Crimes @False @'[EstPopulation, PostedBondFreq] @EstPopulation 
        regressOneWTLS = MR.functionToFoldM $ FR.varWeightedTLS @effs @Crimes @False @'[EstPopulation, PostedBondFreq] @EstPopulation
        regReduceF r k = fmap (M.singleton k) r
        allMR  = (,,,,,) <$> dataMR
                 <*> (regMR $ MR.ReduceFoldM $ regReduceF regressOneBM)
                 <*> (regMR $ MR.ReduceFoldM $ regReduceF regressOneOLS)
                 <*> (regMR $ MR.ReduceFoldM $ regReduceF regressOneWLS)
                 <*> (regMR $ MR.ReduceFoldM $ regReduceF regressOneWLS2)
                 <*> (regMR $ MR.ReduceFoldM $ regReduceF regressOneWTLS) 
    K.logLE K.Info "Regressing Crime Rate on Money Bond Rate"
    (rData, r1, r2,r3,r4,r5) <- FL.foldM allMR countyBondAndCrimeMerged
--    K.logLE K.Info $ "regression (by minimization) results: " <> (T.pack $ show $ fmap (flip FR.prettyPrintRegressionResult 0.95) r1)
    K.logLE K.Info $ "regression (by OLS) results: " <> FR.prettyPrintRegressionResults FR.keyRecordText (M.toList r2) ST.cl95 FR.prettyPrintRegressionResult "\n"
    K.logLE K.Info $ "regression (rates by pop weighted LS) results: " <> FR.prettyPrintRegressionResults FR.keyRecordText (M.toList r3) ST.cl95 FR.prettyPrintRegressionResult "\n"
    K.logLE K.Info $ "regression (counts by inv sqrt pop weighted LS) results: " <> FR.prettyPrintRegressionResults FR.keyRecordText (M.toList r4) ST.cl95 FR.prettyPrintRegressionResult "\n"
    K.logLE K.Info $ "regression (counts by TLS) results: " <> FR.prettyPrintRegressionResults FR.keyRecordText  (M.toList r5) ST.cl95 FR.prettyPrintRegressionResult "\n"
    let rData2016 = F.filterFrame ((== 2016) . F.rgetField @Year) $ F.toFrame rData
        regressionR = fromJust $ M.lookup (FT.recordSingleton @Year 2016) r4
    return $ (rData2016, regressionR, r4)    
--    K.log K.Info $ "data=\n" <> Table.textTable rData
    
  K.logLE K.Info "Creating Doc"
  K.addLucid $ do
    HL.h1_ "Colorado Money Bond Rate vs Crime rate" 
    KL.placeTextSection $ do
      HL.h2_ "Colorado Bond Rates and Crime Rates (preliminary)"
      HL.p_ [HL.class_ "subtitle"] "Adam Conner-Sax"
      HL.p_ "Each county in Colorado issues money bonds and personal recognizance bonds.  For each county I look at the % of money bonds out of all bonds issued and the crime rate.  We have 3 years of data and there are 64 counties in Colorado (each with vastly different populations).  So I've used a population-weighted k-means clustering technique (see notes below) to reduce the number of points to at most 10 per year. Each circle in the plot below represents one cluster of counties with similar money bond and poverty rates.  The size of the circle represents the total population in the cluster."
    KL.placeVisualization "crimeRateVsMoneyBondRateMerged" $ cRVsMBRVL True kmMergedCrimeRateVsMoneyBondRateByYear    
    HL.p_ "Broken down by Colorado's crime categories:"
    KL.placeVisualization "crimeRateVsMoneyBondRateUnMerged" $ cRVsMBRVL False kmCrimeRateVsMoneyBondRateByYearAndType
    KL.placeTextSection $ do
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
    KL.placeTextSection $ do
      HL.p_ "Below I look at the percentage of all bonds which are \"posted\" (posting bond means paying the money bond or agreeing to the terms of the personal recognizance bond ?) rather than the % of money bonds. I use the same clustering technique."
    KL.placeVisualization "crimeRateVsPostedBondRate" $ cRVsPBRVL True kmMergedCrimeRateVsPostedBondRateByYear
    HL.p_ "Broken down by Colorado's crime categories:"
    KL.placeVisualization "crimeRateVsPostedBondRateUnMerged" $ cRVsPBRVL False kmCrimeRateVsPostedBondRateByYearAndType
    KL.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ $ do
          HL.span_ "Posted Bond Rate is [(number of posted money bonds + number of posted PR bonds)/total number of bonds] where that data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv."
        HL.li_ $ do
          HL.span_ "Crime Rate, as above, is crimes/estimated_population where those numbers come from the Colorado crime statistics "
          HL.a_ [HL.href_ "https://coloradocrimestats.state.co.us/"] "web-site."
    HL.p_ "Below I look at the rate of re-offending among people out on bond, here considered in each county along with money-bond rate and clustered as above."       
    KL.placeVisualization "reoffenseRateVsMoneyBondRate" $ rRVsMBRVL True kmReoffenseRateVsMergedMoneyBondRateByYear
    KL.placeTextSection $ do
      HL.p_ "Notes:"
      HL.ul_ $ do
        HL.li_ $ do
          HL.span_ "Reoffense Rate is (number of new offenses for all bonds/total number of posted bonds) where that data comes from "
          HL.a_ [HL.href_ "https://github.com/Data4Democracy/incarceration-trends/blob/master/Colorado_ACLU/4-money-bail-analysis/complete-county-bond.csv"] "complete-county-bond.csv."

    KL.placeTextSection $ do
      HL.h2_ "Regressions"
      HL.p_ "We can use linear regression to investigate the relationship between Crime Rate and the use of money and personal recognizance bonds. We begin by finding a best fit (using population-weighted least squares) to the model "
      KL.latex_ "$c_i = cr (p_{i}) + A (pb_{i}) + e_{i}$" 
      HL.p_ "where, for each county (denoted by the subscipt i), c is the number of crimes, p is the population and pb is the number of posted bonds and e is an error term we seek to minimize. We look at the result for each of the years in the data:"
    KL.placeVisualization "regresssionCoeffs" $ FV.regressionCoefficientPlotMany (T.pack . show . F.rgetField @Year) "Regression Results (by year)" [" cr","A"] (M.toList (fmap FR.regressionResult regressionResMany)) ST.cl95
    HL.p_ "We look at these regressions directly by overlaying this model on the data itself:"
--    H.placeVisualization "regresssionScatter"  $ FV.scatterWithFit @PostedBondPerCapita @CrimeRate errF (FV.FitToPlot "WLS regression" fitF) "test scatter with fit" rData
--    H.placeVisualization "regresssionScatter"  $ FV.scatterWithFit @PostedBondPerCapita @CrimeRate errF (FV.FitToPlot "WLS regression" fitF) "test scatter with fit" rData
    KL.placeVisualization "regresssionScatter"  $ FV.frameScatterWithFit "test scatter with fit" (Just "WLS") regressionRes ST.cl95 rData
    kMeansNotes
--  liftIO $ T.writeFile "analysis/moneyBondRateAndCrimeRate.html" $ TL.toStrict $ htmlAsText

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
kmMoneyBondPctAnalysis :: _
kmMoneyBondPctAnalysis joinedData = K.wrapPrefix "MoneyBondVsPoverty" $ do
  K.logLE K.Info "Doing money bond % vs poverty rate analysis..."
  let sunPovertyRF = FL.premap (F.rgetField @PovertyR) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
      sunMoneyBondRateF = FL.premap (F.rgetField @MoneyBondRate) $ MR.scaleAndUnscale (MR.RescaleNormalize 1) MR.RescaleNone id
      initialCentroids = KM.kMeansPPCentroids @DblX @DblY @TotalPop KM.euclidSq
      unpack = fmap (FT.mutate moneyBondRate) $ MR.unpackGoodRows @[Year,County,OffType,Urbanicity,PovertyR, MoneyBondFreq,TotalBondFreq,TotalPop]
      reduce :: _
      reduce = MR.ReduceM $ \_ -> KM.kMeansOne @PovertyR @MoneyBondRate @TotalPop sunPovertyRF sunMoneyBondRateF 5 initialCentroids (KM.weighted2DRecord @DblX @DblY @TotalPop) KM.euclidSq
      toRec (x, y, z) = (x F.&: y F.&: z F.&: V.RNil) :: F.Record [PovertyR, MoneyBondRate, TotalPop]
      kmByYearF = MR.concatFoldM $ MR.hashableMapReduceFoldM
        (MR.generalizeUnpack unpack)
        (MR.generalizeAssign $ MR.assignKeysAndData @[Year, OffType] @[PovertyR, MoneyBondRate, TotalPop])
        (MR.makeRecsWithKeyM toRec reduce)
      kmByYearUrbF = MR.concatFoldM $ MR.hashableMapReduceFoldM
        (MR.generalizeUnpack unpack)
        (MR.generalizeAssign $ MR.assignKeysAndData @'[Year, OffType, Urbanicity] @[PovertyR, MoneyBondRate, TotalPop])
        (MR.makeRecsWithKeyM toRec reduce)
  (kmByYear, kmByYearUrb) <- FL.foldM ((,) <$> kmByYearF <*> kmByYearUrbF) joinedData
{-  (kmByYear, kmByYearUrb) <- FL.foldM ((,)
                                        <$> KM.kMeans @[Year, OffType] @PovertyR @MoneyBondRate @TotalPop sunPovertyRF sunMoneyBondRateF 5 (KM.kMeansPPCentroids KM.euclidSq) KM.euclidSq
                                        <*> KM.kMeans @[Year, OffType, Urbanicity] @PovertyR @MoneyBondRate @TotalPop sunPovertyRF sunMoneyBondRateF 5 (KM.kMeansPPCentroids KM.euclidSq) KM.euclidSq) kmData -}
  K.logLE K.Info "Creating Doc"
  K.addLucid $ do
    HL.h1_ "Colorado Money Bonds and Poverty" 
    KL.placeTextSection $ do
      HL.h2_ "Colorado Money Bond Rate and Poverty (preliminary)"
      HL.p_ [HL.class_ "subtitle"] "Adam Conner-Sax"
      HL.p_ "Colorado issues two types of bonds when releasing people from jail before trial. Sometimes people are released on a \"money bond\" and sometimes on a personal recognizance bond. We have county-level data of all bonds (?) issued in 2014, 2015 and 2016.  Plotting it all is very noisy (since there are 64 counties in CO) so we use a population-weighted k-means clustering technique to combine similar counties into clusters. In the plots below, each circle represents one cluster of counties and the circle's size represents the total population of all the counties included in the cluster.  We consider felonies and misdemeanors separately."
    KL.placeVisualization "mBondsVspRateByYrFelonies" $ moneyBondPctVsPovertyRateVL False kmByYear
    KL.placeTextSection $ HL.h3_ "Broken down by \"urbanicity\":"
    KL.placeVisualization "mBondsVspRateByYrUrbFelonies" $ moneyBondPctVsPovertyRateVL True kmByYearUrb
    KL.placeTextSection $ do
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
--  liftIO $ T.writeFile "analysis/moneyBondRateAndPovertyRate.html" $ TL.toStrict $ htmlAsText

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

kMeansNotes =  KL.placeTextSection $ do
  HL.h3_ "Some notes on weighted k-means"
  HL.ul_ $ do
    HL.li_ $ do
      HL.a_ [HL.href_ "https://en.wikipedia.org/wiki/K-means_clustering"] "k-means"
      HL.span_ " works by choosing random starting locations for cluster centers, assigning each data-point to the nearest cluster center, moving the center of each cluster to the weighted average of the data-points assigned to the same cluster and then repeating until no data-points move to a new cluster."
    HL.li_ "How do you define distance between points?  Each axis has a different sort of data and it's not clear how to combine differences in each into a meanignful overall distance.  Here, before we hand the data off to the k-means algorithm, we shift and rescale the data so that the set of points has mean 0 and std-deviation 1 in each variable.  Then we use ordinary Euclidean distance, that is the sum of the squares of the differences in each coordinate.  Before plotting, we reverse that scaling so that we can visualize the data in the original. This attempts to give the two variables equal weight in determining what \"nearby\" means for these data points."
    HL.li_ "This clustering happens for each combination plotted."
    HL.li_ $ do
      HL.span_ "k-means is sensitive to the choice of starting points.  Here we use the \"k-means++\" method.  This chooses one of the data points as a starting point.  Then chooses among the remaining points in a way designed to make it likely that the initial centers are widely dispersed.  See "
      HL.a_ [HL.href_ "https://en.wikipedia.org/wiki/K-means%2B%2B"] "here"
      HL.span_ " for more information.  We repeat the k-means clustering with a few different starting centers chosen this way and choose the best clustering, in the sense of minimizing total weighted distance of all points from their cluster centers."


transformF :: forall x rs. (V.KnownField x, x ∈ rs) => (V.Snd x -> V.Snd x) -> F.Record rs -> F.Record rs
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



