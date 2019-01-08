{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
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
module Main where

import           DataSources
import           Frames.Aggregations     as FA
import           Frames.MaybeUtils       as FM

import qualified Control.Foldl           as FL
import           Control.Lens            ((^.))
import           Control.Monad.IO.Class  (MonadIO, liftIO)
import qualified Data.Foldable           as Foldable
import qualified Data.List               as List
import qualified Data.Map                as M
import           Data.Maybe              (catMaybes, fromJust, fromMaybe,
                                          isJust)
import           Data.Proxy              (Proxy (..))
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Vector             as V
import qualified Data.Vinyl              as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Core         as V
import           Data.Vinyl.Curry        (runcurryX)
import qualified Data.Vinyl.Derived      as V
import qualified Data.Vinyl.Functor      as V
import qualified Data.Vinyl.TypeLevel    as V
import           Data.Vinyl.XRec         as V
import           Frames                  ((:.), (&:))
import qualified Frames                  as F
import qualified Frames.CSV              as F
import qualified Frames.Dsv              as F
import qualified Frames.InCore           as FI
import qualified Frames.ShowCSV          as F
import qualified Frames.TH               as F
import           GHC.TypeLits            (KnownSymbol, Symbol)
import qualified Pipes                   as P
import qualified Pipes.Prelude           as P


type CO_AnalysisVERA_Cols = [Year, State, TotalPop, TotalJailAdm, TotalJailPop, TotalPrisonAdm, TotalPrisonPop]

F.tableTypes' (F.rowGen fipsByCountyFP) { F.rowTypeName = "FIPSByCountyRenamed", F.columnNames = ["fips","County","State"]}

type instance FI.VectorFor (Maybe a) = V.Vector


justsFromRec :: V.RMap fs => F.Record fs -> F.Rec (Maybe :. F.ElField) fs
justsFromRec = V.rmap (V.Compose . Just)

main :: IO ()
main = do
  -- create streams which are filtered to CO
  let veraData :: F.MonadSafe m => P.Producer (MaybeRow IncarcerationTrends)  m ()
      veraData = F.readDsvTableMaybeOpt F.defaultParser veraTrendsFP  P.>-> P.filter (filterMaybeField (Proxy @State) "CO")
      povertyData :: F.MonadSafe m => P.Producer SAIPE m ()
      povertyData = F.readDsvTable censusSAIPE_FP P.>-> P.filter (filterField (Proxy @Abbreviation) "\"CO\"")
      fipsByCountyData :: F.MonadSafe m => P.Producer FIPSByCountyRenamed m ()
      fipsByCountyData = F.readDsvTable fipsByCountyFP  P.>-> P.filter (filterField (Proxy @State) "CO")
      countyBondCO_Data :: F.MonadSafe m => P.Producer (MaybeRow CountyBondCO) m ()
      countyBondCO_Data = F.readDsvTableMaybeOpt F.defaultParser countyBondCO_FP
      countyDistrictCO_Data :: F.MonadSafe m => P.Producer CountyDistrictCO m ()
      countyDistrictCO_Data = F.readDsvTable countyDistrictCrosswalkCO_FP
  -- load streams into memory for joins, subsetting as we go
  fipsByCountyFrame <- F.inCoreAoS $ fipsByCountyData P.>-> P.map (F.rcast @[Fips,County]) -- get rid of state col
  povertyFrame <- F.inCoreAoS $ povertyData P.>-> P.map (F.rcast @[Fips, Year, MedianHI,MedianHIMOE,PovertyR])
  countyBondFrameM {- :: F.Frame (F.Rec (Maybe :. F.ElField) (F.RecordColumns CountyBondCO)) -} <- fmap F.boxedFrame $ F.runSafeEffect $ P.toListM countyBondCO_Data
  veraFrameM <- fmap F.boxedFrame $ F.runSafeEffect $ P.toListM veraData -- no subset here, so this is a LARGE row
  countyDistrictFrame <- F.inCoreAoS countyDistrictCO_Data
  let countyBondPlusFIPS = FM.leftJoinMaybe (Proxy @'[County]) countyBondFrameM (justsFromRec <$> fipsByCountyFrame)
      countyBondPlusFIPSAndDistrict = FM.leftJoinMaybe (Proxy @'[County]) (F.boxedFrame countyBondPlusFIPS) (justsFromRec <$> countyDistrictFrame)
      countyBondPlusFIPSAndSAIPE = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndDistrict) (justsFromRec <$> povertyFrame)
      countyBondPlusFIPSAndSAIPEAndVera = FM.leftJoinMaybe (Proxy @[Fips, Year]) (F.boxedFrame countyBondPlusFIPSAndSAIPE) veraFrameM
  FM.writeCSV_Maybe "data/countyBondPlusFIPS.csv" countyBondPlusFIPS
  FM.writeCSV_Maybe "data/countyBondPlusSAIPE.csv" countyBondPlusFIPSAndSAIPE
  FM.writeCSV_Maybe "data/countyBondPlus.csv" countyBondPlusFIPSAndSAIPEAndVera
--  coloradoRowCheck <- F.runSafeEffect $ FL.purely P.fold (goodDataByKey  [F.pr1|Year|]) coloradoTrendsData
--  putStrLn $ "(CO rows, CO rows with all fields) = " ++ show coloradoRowCheck
  return ()


