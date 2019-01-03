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
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ConstraintKinds #-}
module Main where

import           TrendsDataTypes

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Applicative (liftA2)
import qualified Data.List            as List
import qualified Data.Map             as M
import           Data.Maybe           (fromJust, fromMaybe, isJust, catMaybes)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec      as V
import qualified Data.Vinyl.Core as V
import qualified Data.Vinyl.Class.Method as V
import Data.Vinyl.Lens (type (∈))
import qualified Dhall                as D
import           Frames               ((:.), (&:))
import qualified Frames               as F
import qualified Frames.CSV           as F
import qualified Frames.ShowCSV           as F
import qualified Frames.Melt           as F
import qualified Frames.InCore        as FI
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import Control.Arrow (second)
import Data.Proxy (Proxy(..))
import qualified Data.Foldable as Fold
import qualified Data.Vector as V
import GHC.TypeLits (KnownSymbol)


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

data Bin2DT = Bin2D (Int, Int) deriving (Show, Eq, Ord)
type instance FI.VectorFor Bin2DT = V.Vector
instance F.ShowCSV Bin2DT where
  showCSV = T.pack . show

F.declareColumn "ImprisonedPerCrimeRate" ''Double
F.declareColumn "CrimeRate" ''Double
F.declareColumn "IncarcerationRate" ''Double
F.declareColumn "PrisonAdmRate" '' Double
F.declareColumn "Gender" ''GenderT
F.declareColumn "Bin2D" ''Bin2DT

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
aggregateF :: forall rs ks as b cs f g. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
           => Proxy ks
           -> (F.Rec g rs -> f (F.Record as))
           -> (F.Record as -> b -> b)
           -> b
           -> (b -> F.Record cs)
           -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateF _ unpack process initial extract =
  let getKey :: F.Record as -> F.Record ks
      getKey = F.rcast
  in FL.Fold (aggregateGeneral unpack getKey process initial) M.empty (F.toFrame . fmap (uncurry V.rappend . second extract) . M.toList) 
--
-- We use a GADT here so that each constructor can carry the proofs of numerical type.  We don't want RescaleNone to require RealFloat. 
data RescaleType a where
  RescaleNone :: RescaleType a
  RescaleMean :: RealFloat a => RescaleType a
  RescaleMedian :: (Ord a, Real a) => RescaleType a
  RescaleNormalize :: RealFloat a => RescaleType a
  RescaleGiven :: (a, Double) -> RescaleType a

-- NB: Bins are unscaled; scaling function to be applied after binning  
data BinsWithRescale a = BinsWithRescale { bins :: [a],  shift :: a, scale :: Double} -- left in a form where a need not be a type that supports division 

rescale :: (Num a, Foldable f) => RescaleType a -> f a -> (a, Double)
rescale RescaleNone _ = (0,1)
rescale (RescaleGiven x) _ = x
rescale RescaleMean fa = (0, realToFrac $ FL.fold FL.mean fa)
rescale RescaleNormalize fa =
  let (shift, scale) = FL.fold ((,) <$> FL.mean <*> FL.std) fa
  in (shift, realToFrac scale)
rescale RescaleMedian fa =
  let l = List.sort $ FL.fold FL.list fa
      n = List.length l
      s =  case n of
        0 -> 1
        _ -> let m = n `div` 2 in if (odd n) then realToFrac (l !! m) else realToFrac (l List.!! m + l List.!! (m - 1))/2.0
  in (0,s/100)

binField :: forall rs ks f w fl ft wl wt. (KnownSymbol wl, KnownSymbol fl, Num ft, Ord ft, Integral wt, Num wt,
                                           Ord (F.Record ks), ks F.⊆ rs, F.ElemOf rs f, F.ElemOf rs w, f ~ (fl F.:-> ft), w ~ (wl F.:-> wt))
         => Int -> Proxy ks -> Proxy '[f,w] -> RescaleType ft -> FL.Fold (F.Record rs) (M.Map (F.Record ks) (BinsWithRescale ft))
binField n _ _ rt =
  let getKey :: F.Record rs -> F.Record ks = F.rcast
      getFW :: F.Record rs -> F.Record '[f,w] = F.rcast
      process :: F.Record rs -> [(ft,wt)] -> [(ft,wt)]
      process r l = V.runcurryX (\f w -> (f,w) : l) $ getFW r
      extract :: RescaleType ft -> [(ft,wt)] -> BinsWithRescale ft
      extract rt l =
        let compFst (x,_) (x',_) = compare x x'
            scaleInfo = rescale rt (fst <$> l)
            (totalWeight, listWithSummedWeights) = List.mapAccumL (\sw (f,w) -> (sw+w, (f,w,sw+w))) 0 $ List.sortBy compFst l
            weightPerBin :: Double = fromIntegral totalWeight/fromIntegral n
            lowerBounds :: Num wt => [(b,wt,wt)] -> [b] -> [b]
            lowerBounds x bs = case List.null x of
              True -> bs
              False ->
                let nextLB = (\(x,_,_) -> x) . head $ x
                    newX = List.dropWhile (\(_,_,sw) -> (fromIntegral sw) < weightPerBin * (fromIntegral $ List.length bs + 1)) x
                    newBS = bs ++ [nextLB]
                in lowerBounds newX newBS
        in BinsWithRescale (lowerBounds listWithSummedWeights []) (fst scaleInfo) (snd scaleInfo)
  in FL.Fold (aggregateGeneral V.Identity getKey process []) M.empty (fmap $ extract rt)

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

type TrendsPovRow = '[Year, Fips, State, CountyName, Urbanicity, TotalPop, CrimeRate, IncarcerationRate, ImprisonedPerCrimeRate]
--TotalPop, TotalPop15To64, TotalPrisonPop, TotalPrisonAdm, IndexCrime]

trendsRowForPovertyAnalysis :: Monad m => P.Pipe MaybeITrends (F.Rec F.ElField TrendsPovRow) m ()
trendsRowForPovertyAnalysis = do
  r <- P.await
  let dataWeNeedM :: Maybe (F.Rec F.ElField '[Year, Fips, State, CountyName, Urbanicity, TotalPop, TotalPop15to64, TotalPrisonPop, TotalPrisonAdm, IndexCrime]) = F.recMaybe $ F.rcast r
  case dataWeNeedM of
    Nothing -> trendsRowForPovertyAnalysis
    Just x -> do
      let newRow = V.runcurryX (\y f s c u tp tp' tpp tpa ic -> y &: f &: s &: c &: u &: tp &: (fromIntegral ic/fromIntegral tp) &: (fromIntegral tpp/fromIntegral tp) &: (if ic == 0 then 0 :: Double else (fromIntegral tpa/fromIntegral ic)) &: V.RNil) x  
      P.yield newRow >> trendsRowForPovertyAnalysis

{-
listToBinLookup :: Ord a => [a] - > (a -> Int)
listToBinLookup = sortedListToBinLookup . List.sort

data BinData a = BinData { val :: a, bin :: Int }
instance Ord a => Ord (BinData a) where
  compare = compare . val
  
data BinLookupTree a = Leaf | Node (BinLookupTree a) (BinData a) (BinLookupTree a) 

sortedListToBinLookup :: Ord a => [a] -> a -> Int
sortedListToBinLookup as a =
  let tree xs = case List.length of
        0 -> Leaf
        l -> let (left,right) = List.splitAt (l `quot` 2) xs in Node (tree left) (head right) (tree $ tail right)
      searchTree = tree $ fmap (\(a,n) -> BinData a n) $ List.zip as [1..]
      findBin :: Int -> a -> BinLookupTree a -> Int
      findBin n a Leaf = n
      findBin n a (Node l (BinData a' m) r) = if (a > a') then findBin m a r else findBin n a l
  in findBin 0 a searchTree
-}
-- NB: a can't be less than the 0th element because we build it that way.  So we drop it
sortedListToBinLookup' :: Ord a => [a] -> a -> Int
sortedListToBinLookup' as a = let xs = tail as in 1 + (fromMaybe (List.length xs) $ List.findIndex (>a) xs)

main :: IO ()
main = do
  config <- D.input D.auto "./config/explore-data.dhall"
  let trendsData = F.readTableMaybe $ trendsCsv config
    --aggregationAnalyses trendsData
  let povertyData = F.readTable $ povertyCsv config -- no missing data here
  incomePovertyJoinData trendsData povertyData

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

{-
doScatterMerge :: forall keys rs f. (Foldable f, (keys V.++ '[Bin2D] V.++ '[MedianHI,IncarcerationRate,TotalPop]) ~ (keys V.++ '[Bin2D,MedianHI,IncarcerationRate,TotalPop]))
               => Proxy keys -> f (F.Record rs) -> FilePath -> IO ()
doScatterMerge _ trendsWithPovertyF path = do
  let binIncomeFold = binField 10 (Proxy @keys) (Proxy @[MedianHI, TotalPop]) RescaleMedian 
      binIncarcerationRateFold = binField 10 (Proxy @keys) (Proxy @[IncarcerationRate, TotalPop]) RescaleNone 
      (incomeBins, incarcerationRateBins)
        = FL.fold ((,) <$> binIncomeFold <*> binIncarcerationRateFold) trendsWithPovertyF
      scatterMergeFold = scatterMerge (Proxy @keys) (Proxy @[MedianHI, IncarcerationRate, TotalPop]) round id incomeBins incarcerationRateBins
      scatterMergeFrame :: F.FrameRec (keys V.++ '[MedianHI, IncarcerationRate, TotalPop]) = F.rcast <$> FL.fold scatterMergeFold trendsWithPovertyF 
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByUrbanicityAndYear.csv" scatterMergeFrame
-}
  
incomePovertyJoinData trendsData povertyData = do
  trendsForPovFrame <- F.inCoreAoS $ trendsData P.>-> trendsRowForPovertyAnalysis
  povertyFrame :: F.Frame SAIPE <- F.inCoreAoS povertyData
  let trendsWithPovertyF = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[Fips,Year] trendsForPovFrame povertyFrame
  F.writeCSV "data/trendsWithPoverty.csv" trendsWithPovertyF
  let binIYrFold = binField 10 [F.pr1|Year|] (Proxy @[MedianHI, TotalPop]) RescaleMedian 
      binIRYrFold = binField 10 [F.pr1|Year|] (Proxy @[IncarcerationRate, TotalPop]) RescaleNone
      binIStateYrFold = binField 10 (Proxy @[State,Year]) (Proxy @[MedianHI, TotalPop]) RescaleMedian 
      binIRStateYrFold = binField 10 (Proxy @[State,Year]) (Proxy @[IncarcerationRate, TotalPop]) RescaleNone
      binIUrbYrFold = binField 10 (Proxy @[Urbanicity,Year]) (Proxy @[MedianHI, TotalPop]) RescaleMedian 
      binIRUrbYrFold = binField 10 (Proxy @[Urbanicity,Year]) (Proxy @[IncarcerationRate, TotalPop]) RescaleNone
      
      (iBinsYr, iRBinsYr, iBinsStateYr, iRBinsStateYr, iBinsUrbYr, iRBinsUrbYr)
        = FL.fold ((,,,,,) <$> binIYrFold <*> binIRYrFold <*> binIStateYrFold <*> binIRStateYrFold <*> binIUrbYrFold <*> binIRUrbYrFold) trendsWithPovertyF
      scatterMergeYrFold = scatterMerge [F.pr1|Year|] (Proxy @[MedianHI, IncarcerationRate, TotalPop]) round id iBinsYr iRBinsYr
      scatterMergeStateYrFold = scatterMerge (Proxy @[State,Year]) (Proxy @[MedianHI, IncarcerationRate, TotalPop]) round id iBinsStateYr iRBinsStateYr
      scatterMergeUrbYrFold = scatterMerge (Proxy @[Urbanicity,Year]) (Proxy @[MedianHI, IncarcerationRate, TotalPop]) round id iBinsUrbYr iRBinsUrbYr
      (smYr, smStateYr, smUrbYr) = FL.fold ((,,) <$> scatterMergeYrFold <*> scatterMergeStateYrFold <*> scatterMergeUrbYrFold) trendsWithPovertyF
      smYearF :: F.FrameRec '[Year, MedianHI, IncarcerationRate, TotalPop] = F.rcast <$> smYr
      smStateYearF :: F.FrameRec '[State, Year, MedianHI, IncarcerationRate, TotalPop] = F.rcast <$> smStateYr
      smUrbYearF :: F.FrameRec '[Urbanicity, Year, MedianHI, IncarcerationRate, TotalPop] = F.rcast <$> smUrbYr
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByYear.csv" smYearF
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByStateAndYear.csv" smStateYearF
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByUrbanicityAndYear.csv" smUrbYearF

type X = "x" F.:-> Double
type Y = "y" F.:-> Double

type UseCols ks x y w = ks V.++ '[x,y,w]
type UseColsC ks x y w = (ks F.⊆ UseCols ks x y w, x ∈ UseCols ks x y w, y ∈ UseCols ks x y w, w ∈ UseCols ks x y w)
type OutKeyCols ks = ks V.++ '[Bin2D]
type BinnedDblCols ks w = ks V.++ '[Bin2D, X, Y, w]
type BinnedDblColsC ks w = (Bin2D ∈ BinnedDblCols ks w, X ∈ BinnedDblCols ks w, Y ∈ BinnedDblCols ks w, w ∈ BinnedDblCols ks w)
type BinnedResultCols ks x y w = ks V.++ '[Bin2D, x, y, w]
type FieldOfType x a = (x ~ (V.Fst x F.:-> a), KnownSymbol (V.Fst x)) 
type ScatterMergeable rs ks x y w a b wt = (ks F.⊆ rs,
                                            Ord (F.Record ks),
                                            FI.RecVec (BinnedResultCols ks x y w),
                                            x ∈ rs, FieldOfType x a, Real a,
                                            y ∈ rs, FieldOfType y b, Real b,
                                            w ∈ rs, FieldOfType w wt, Real wt,
                                            BinnedDblColsC ks w,
                                            UseCols ks x y w F.⊆ rs, UseColsC ks x y w,
                                            OutKeyCols ks F.⊆ BinnedDblCols ks w,
                                            Ord (F.Record (OutKeyCols ks)),
                                            ((OutKeyCols ks) V.++ '[x,y,w]) ~ (BinnedResultCols ks x y w))

scatterMerge :: forall rs ks x y w a b wt. ScatterMergeable rs ks x y w a b wt
             => Proxy ks
             -> Proxy '[x,y,w]
             -> (Double -> a) -- when we put the averaged data back in the record with original types we need to convert back
             -> (Double -> b)
             -> M.Map (F.Record ks) (BinsWithRescale a)
             -> M.Map (F.Record ks) (BinsWithRescale b)
             -> FL.Fold (F.Record rs) (F.FrameRec (BinnedResultCols ks x y w))
scatterMerge _ _ toX toY xBins yBins =
  let binningInfo :: Real c => BinsWithRescale c -> (c -> Int, c -> Double)
      binningInfo (BinsWithRescale bs shift scale) = (sortedListToBinLookup' bs, (\x -> realToFrac (x - shift)/scale))
      xBinF = binningInfo <$> xBins
      yBinF = binningInfo <$> yBins
      trimRow :: F.Record rs -> F.Record (UseCols ks x y w) = F.rcast
      binRow :: F.Record (UseCols ks x y w) -> F.Record (BinnedDblCols ks w) -- 'ks ++ [Bin2D,X,Y,w]
      binRow r =
        let key :: F.Record ks = F.rcast r
            xyw :: F.Record '[x,y,w] = F.rcast r
            (xBF, xSF) = fromMaybe (const 0, realToFrac) $ M.lookup key xBinF
            (yBF, ySF) = fromMaybe (const 0, realToFrac) $ M.lookup key yBinF
            binnedAndScaled :: F.Record '[Bin2D, X, Y, w] = V.runcurryX (\x y w -> Bin2D (xBF x, yBF y) &: xSF x &: ySF y &: w &: V.RNil) xyw
        in key V.<+> binnedAndScaled 
      wgtdSum :: (Double, Double, wt) -> F.Record (BinnedDblCols ks w) -> (Double, Double, wt)
      wgtdSum (wX, wY, totW) r =
        let xyw :: F.Record '[X,Y,w] = F.rcast r
        in  V.runcurryX (\x y w -> let w' = realToFrac w in (wX + (w' * x), wY + (w' * y), totW + w)) xyw
      extract :: [F.Record (BinnedDblCols ks w)] -> F.Record '[x,y,w]  
      extract = FL.fold (FL.Fold wgtdSum (0, 0, 0) (\(wX, wY, totW) -> let totW' = realToFrac totW in toX (wX/totW') &:  toY (wY/totW') &: totW &: V.RNil))
  in aggregateF (Proxy @(OutKeyCols ks)) (V.Identity . binRow . trimRow) (:) [] extract 
      
  
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
