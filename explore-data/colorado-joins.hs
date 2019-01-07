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

F.tableTypes' (F.rowGen fipsByCountyFP) { F.rowTypeName = "FIPSByCountyRenamed", F.columnNames = ["FIPS","County","State"]}

type instance FI.VectorFor (Maybe a) = V.Vector

{-
--We should package the below up, along with some produceCSV and writeCSV functions
instance (F.ShowCSV a) => F.ShowCSV (Maybe a) where
  showCSV = fromMaybe "NA" . fmap F.showCSV

type AddMaybe rs = V.MapTyCon Maybe rs

type family RDeleteL ds rs where
  RDeleteL '[] rs = rs
  RDeleteL (d ': ds) rs = RDeleteL ds (F.RDelete d rs)

unMaybeKeys :: forall ks rs as.(ks F.⊆ rs, as ~ RDeleteL ks rs, as F.⊆ rs, V.RPureConstrained V.KnownField as, V.RecApplicative as, V.RApply as, V.RMap as)
  => Proxy rs -> Proxy ks -> F.Rec (Maybe F.:. F.ElField) rs -> F.Record (ks V.++ (AddMaybe as))
unMaybeKeys _ _ mr =
  let keys = fromJust $ F.recMaybe $ F.rcast @ks mr
      remainder = V.rsequenceInFields $ F.rcast @as mr
 in keys `V.rappend` remainder

-- end of MaybeUtils
-}

main :: IO ()
main = do
  -- create streams which are filtered to CO and cols we need for this
  let --trendsData :: F.MonadSafe m => P.Producer (MaybeRow IncarcerationTrends)  m ()
      --trendsData = F.readDsvTableMaybeOpt F.defaultParser veraTrendsFP  P.>-> P.filter (filterMaybeField (Proxy @State) "CO") -- some data is missing so we use the Maybe form
--      coloradoTrendsData = trendsData P.>-> P.map (F.rcast @CO_AnalysisVERA_Cols)
      povertyData :: F.MonadSafe m => P.Producer SAIPE m ()
      povertyData = F.readDsvTable censusSAIPE_FP P.>-> P.filter (filterField (Proxy @Abbreviation) "CO")
      fipsByCountyData :: F.MonadSafe m => P.Producer FIPSByCountyRenamed m ()
      fipsByCountyData = F.readDsvTable fipsByCountyFP  P.>-> P.filter (filterField (Proxy @State) "CO")
      countyBondCO_Data :: F.MonadSafe m => P.Producer (MaybeRow CountyBondCO) m ()
      countyBondCO_Data = F.readDsvTableMaybeOpt F.defaultParser countyBondCO_FP

      countyBondData = countyBondCO_Data P.>-> P.map (FM.unMaybeKeys (Proxy @[County,Year]))
  -- load streams into memory for joins
  countyBondFrame <- F.inCoreAoS countyBondData
  fipsByCountyFrame <- F.inCoreAoS $ fipsByCountyData P.>-> P.map (F.rcast @[FIPS,County]) -- get rid of state col
  povertyFrame <- F.inCoreAoS povertyData
  let countyBondPlusFIPS = F.leftJoin @'[County] countyBondFrame fipsByCountyFrame
      countyBondPlusFIPSFrame = F.toFrame $ fmap (FM.unMaybeKeys (Proxy @[FIPS, Year])) $ countyBondPlusFIPS
--      countyBondPlusFIPSAndSAIPE = F.leftJoin @'[FIPS, Year] countyBondPlusFIPSFrame povertyFrame
  --F.runSafeEffect $ FM.produceCSV_Maybe countyBondPlus P.>-> P.print -- P.map T.pack P.>-> F.consumeTextLines "data/countyBondPlusFIPS.csv"
  putStrLn $ show countyBondPlusFIPS -- P.>-> P.print
  FM.writeCSV_Maybe "data/countyBondPlusFIPS.csv" countyBondPlusFIPS
--  FM.writeCSV_Maybe "data/countyBondPlusSAIPE.csv" countyBondPlusFIPSAndSAIPE
--  coloradoRowCheck <- F.runSafeEffect $ FL.purely P.fold (goodDataByKey  [F.pr1|Year|]) coloradoTrendsData
--  putStrLn $ "(CO rows, CO rows with all fields) = " ++ show coloradoRowCheck
  return ()

{-
pFilterMaybe :: Monad m => (a -> Maybe b) -> P.Pipe a b m ()
pFilterMaybe f =  P.map f P.>-> P.filter isJust P.>-> P.map fromJust


maybeTest :: (a -> Bool) -> Maybe a -> Bool
maybeTest t = maybe False t

fipsFilter x = maybeTest (== x) . V.toHKD . F.rget @Fips -- use F.rgetField?
stateFilter s = maybeTest (== s) . V.toHKD . F.rget @State
yearFilter y = maybeTest (== y) . V.toHKD . F.rget @Year
-}

