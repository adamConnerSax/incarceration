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
module Main where

import           DataSources
import           Frames.Aggregations        as FA
import           Frames.KMeans              as KM
import           Frames.MaybeUtils          as FM

import           Control.Arrow              ((&&&))
import qualified Control.Foldl              as FL
--import           Control.Monad.IO.Class     (MonadIO, liftIO)
import qualified Data.Aeson.Encode.Pretty   as A
import qualified Data.ByteString.Lazy.Char8 as BS
import           Data.Functor.Identity      (runIdentity)
import qualified Data.List                  as List
import           Data.Maybe                 (catMaybes)
import           Data.Monoid                ((<>))
import           Data.Proxy                 (Proxy (..))
import           Data.Text                  (Text)
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import qualified Data.Text.IO               as T
import qualified Data.Text.Lazy             as TL
import qualified Data.Vector                as V
import qualified Data.Vinyl                 as V
import           Data.Vinyl.Curry           (runcurryX)
import qualified Data.Vinyl.Functor         as V
--import qualified Data.Vinyl.Derived       as V
import           Data.Vinyl.Lens            (type (∈))
import           Frames                     ((:.), (&:))
import qualified Frames                     as F
import qualified Frames.CSV                 as F
import qualified Frames.InCore              as FI
import qualified Frames.ShowCSV             as F
import qualified Frames.TH                  as F
import qualified Graphics.Vega.VegaLite     as GV
import qualified Html                       as H
import qualified Html.Attribute             as HA
import qualified Pipes                      as P
import qualified Pipes.Prelude              as P


-- stage restriction means this all has to be up top
F.tableTypes' (F.rowGen fipsByCountyFP) { F.rowTypeName = "FIPSByCountyRenamed", F.columnNames = ["fips","County","State"]}
F.declareColumn "MoneyPct" ''Double

type CO_AnalysisVERA_Cols = [Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop]

type instance FI.VectorFor (Maybe a) = V.Vector

justsFromRec :: V.RMap fs => F.Record fs -> F.Rec (Maybe :. F.ElField) fs
justsFromRec = V.rmap (V.Compose . Just)

main :: IO ()
main = do
  -- create streams which are filtered to CO
  let parserOptions = F.defaultParser { F.quotingMode =  F.RFC4180Quoting ' ' }
      veraData :: F.MonadSafe m => P.Producer (FM.MaybeRow IncarcerationTrends)  m ()
      veraData = F.readTableMaybeOpt F.defaultParser veraTrendsFP  P.>-> P.filter (filterMaybeField (Proxy @State) "CO")
      povertyData :: F.MonadSafe m => P.Producer SAIPE m ()
      povertyData = F.readTableOpt parserOptions censusSAIPE_FP P.>-> P.filter (filterField (Proxy @Abbreviation) "CO")
      fipsByCountyData :: F.MonadSafe m => P.Producer FIPSByCountyRenamed m ()
      fipsByCountyData = F.readTableOpt parserOptions fipsByCountyFP  P.>-> P.filter (filterField (Proxy @State) "CO")
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
  kmBondRatevsCrimeRateAnalysis countyBondPlusFIPSAndSAIPEAndVera



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



kmMoneyBondPctAnalysis joinedData = do
      -- this does two things.  Creates the type for rcast to infer and then mutates it.  Those should likely be separate.
  let mutateData :: F.Record [Year,County,Urbanicity,PovertyR, MoneyBondFreq,TotalBondFreq,TotalPop] -> F.Record [Year,County,Urbanicity,PovertyR,MoneyPct,TotalPop]
      mutateData  = runcurryX (\y c u pr mbf tbf tp -> y &: c &: u &: pr &: (realToFrac mbf)/(realToFrac tbf) &: tp &: V.RNil)
      kmData = fmap mutateData . catMaybes $ fmap (F.recMaybe . F.rcast) joinedData --countyBondPlusFIPSAndSAIPEAndVera
      dataProxy = Proxy @[PovertyR,MoneyPct,TotalPop]
      sunXF = FL.premap (F.rgetField @PovertyR &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
      sunYF = FL.premap (F.rgetField @MoneyPct &&& F.rgetField @TotalPop) $ FA.weightedScaleAndUnscale (FA.RescaleNormalize 1) FA.RescaleNone id
      kmMoneyBondRatevsPovertyRate proxy_ks = KM.kMeans proxy_ks dataProxy sunXF sunYF 5 KM.partitionCentroids KM.euclidSq
      kmByYearUrb = runIdentity $ FL.foldM (kmMoneyBondRatevsPovertyRate (Proxy @[Year,Urbanicity])) kmData
  F.writeCSV "data/kMeansCOMoneyBondRatevsPovertyRateByYearAndUrbanicity.csv" kmByYearUrb
  T.writeFile "data/test.html" $ TL.toStrict $ H.renderText $ makeHtmlReport $ testVis kmByYearUrb

testVisId :: Text = "vis"

makeHtmlReport testVisScript =
  H.html_
  ( H.head_
      (
        H.title_ ("Test Html Report" :: Text)
        H.# H.script_A (HA.src_ ("https://cdn.jsdelivr.net/npm/vega@4.4.0" :: Text)) ()
        H.# H.script_A (HA.src_ ("https://cdn.jsdelivr.net/npm/vega-lite@3.0.0-rc11" :: Text)) ()
        H.# H.script_A (HA.src_ ("https://cdn.jsdelivr.net/npm/vega-embed@3.28.0" :: Text)) ()
      )
    H.# H.body_
    (
      H.div_A (HA.id_ testVisId) ()
      H.# testVisScript
    )
  )

testVis dataRecords =
  let desc = "Vega Lite Attempt"
      castRow =  F.rcast @[Year, Urbanicity,PovertyR, MoneyPct,TotalPop]
      toDataRow :: F.Record [Year, Urbanicity, PovertyR, MoneyPct, TotalPop] -> [GV.DataRow]
      toDataRow = runcurryX (\y u pr mp tp -> GV.dataRow [("year", GV.Str (T.pack $ show y)),
                                                          ("urbanicity",GV.Str u),
                                                          ("povertyR", GV.Number pr),
                                                          ("moneyPct", GV.Number mp),
                                                          ("total_pop", GV.Number (realToFrac tp))] [])
      rows = List.concat $ fmap (toDataRow . transformF @MoneyPct (*100) . castRow) (FL.fold FL.list dataRecords)
      dat = GV.dataFromRows [] rows
--      dat = GV.dataFromUrl  "file:///Users/adam/DataScience/incarceration/data/kMeansCOMoneyBondRatevsPovertyRateByYearAndUrbanicity.csv"
      enc = GV.encoding
        . GV.position GV.X [ GV.PName "povertyR", GV.PmType GV.Quantitative]
        . GV.position GV.Y [ GV.PName "moneyPct", GV.PmType GV.Quantitative]
        . GV.color [ GV.MName "year", GV.MmType GV.Nominal]
        . GV.size [ GV.MName "total_pop", GV.MmType GV.Quantitative]
        . GV.column [GV.FName "urbanicity", GV.FmType GV.Nominal]
      title = GV.name "% of money bonds (out of money and personal recognizance bonds) by year and urbanicity in CO"
--      xScale = GV.configuration GV.Scale [
      vl = GV.toVegaLite
        [ GV.description desc
        , title
        , GV.background "white"
        , GV.mark GV.Point []
        , enc []
        , GV.height 300
        , GV.width 200
        , dat -- always last so script is more readable
        ]
      vegaScript :: Text = T.decodeUtf8 $ BS.toStrict $ A.encodePretty $ GV.fromVL vl
      script = "var vlSpec=\n" <> vegaScript <> ";\n" <> "vegaEmbed(\'#" <> testVisId <> "\',vlSpec);"
  in H.script_A (HA.type_ ("text/javascript" :: Text) ) (H.Raw script)

transformF :: forall x rs. (V.KnownField x, x ∈ rs) => (FType x -> FType x) -> F.Record rs -> F.Record rs
transformF f r = F.rputField @x (f $ F.rgetField @x r) r

class ToVLDateTime x where
  toVLDateTime :: x -> [GV.DateTime]

--instance ToVLDateTime   

class ToVLDataValue x where
  toVLDataValue :: x -> (Text, GV.DataValue)

instance ToVLDataValue (F.ElField '(s, Int)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ realToFrac $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Double)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Float)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Number $ realToFrac $ V.getField x)

instance ToVLDataValue (F.ElField '(s, String)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Text)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ V.getField x)

instance ToVLDataValue (F.ElField '(s, Bool)) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Boolean $ V.getField x)



--  FM.writeCSV_Maybe "data/countyBondPlusFIPS.csv" countyBondPlusFIPS
--  FM.writeCSV_Maybe "data/countyBondPlusSAIPE.csv" countyBondPlusFIPSAndSAIPE
--  FM.writeCSV_Maybe "data/countyBondPlus.csv" countyBondPlusFIPSAndSAIPEAndVera
--  coloradoRowCheck <- F.runSafeEffect $ FL.purely P.fold (goodDataByKey  [F.pr1|Year|]) coloradoTrendsData
--  putStrLn $ "(CO rows, CO rows with all fields) = " ++ show coloradoRowCheck



