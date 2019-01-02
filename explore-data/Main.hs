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

--data BinType a = EqualSpaceBin | EqualWeightBin (Num b => a -> b)    
binField :: forall rs ks f w fl ft wl wt. (KnownSymbol wl, KnownSymbol fl, Ord ft, Integral wt, Num wt,
                                           Ord (F.Record ks), ks F.⊆ rs, F.ElemOf rs f, F.ElemOf rs w, f ~ (fl F.:-> ft), w ~ (wl F.:-> wt))
         => Int -> Proxy ks -> Proxy '[f,w] -> FL.Fold (F.Record rs) (M.Map (F.Record ks) [ft])
binField n _ _  =
  let getKey :: F.Record rs -> F.Record ks = F.rcast
      getFW :: F.Record rs -> F.Record '[f,w] = F.rcast
      process :: F.Record rs -> [(ft,wt)] -> [(ft,wt)]
      process r l = V.runcurryX (\f w -> (f,w) : l) $ getFW r
      extract :: Num wt => [(ft,wt)] -> [ft]
      extract l =
        let compFst (x,_) (x',_) = compare x x'
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
        in lowerBounds listWithSummedWeights []
  in FL.Fold (aggregateGeneral V.Identity getKey process []) M.empty (fmap extract)

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
  
incomePovertyJoinData trendsData povertyData = do
  trendsForPovFrame <- F.inCoreAoS $ trendsData P.>-> trendsRowForPovertyAnalysis
  povertyFrame :: F.Frame SAIPE <- F.inCoreAoS povertyData
  let trendsWithPovertyF = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[Fips,Year] trendsForPovFrame povertyFrame
      binIncomeFold = binField 10 (Proxy @[Year,Urbanicity]) (Proxy @[MedianHI, TotalPop]) 
      binIncarcerationRateFold = binField 10 (Proxy @[Year,Urbanicity]) (Proxy @[IncarcerationRate, TotalPop]) 
      (incomeBins, incarcerationRateBins)
        = FL.fold ((,) <$> binIncomeFold <*> binIncarcerationRateFold) trendsWithPovertyF
      scatterMergeFold = scatterMerge (Proxy @[Year, Urbanicity]) (Proxy @[MedianHI, IncarcerationRate, TotalPop]) round id incomeBins incarcerationRateBins
      scatterMergeFrame :: F.FrameRec '[Year, Urbanicity, MedianHI, IncarcerationRate, TotalPop] = F.rcast <$> FL.fold scatterMergeFold trendsWithPovertyF 
  F.writeCSV "data/trendsWithPoverty.csv" trendsWithPovertyF
  F.writeCSV "data/scatterMergeIncarcerationRate_vs_MedianHIByUrbanicityAndYear.csv" scatterMergeFrame


--type Bins =  "bins" F.:-> (Int, Int) 

scatterMerge :: forall ks useCols outKeyCols x y w xl yl wl wt rs binnedCols a b.
                  (ks F.⊆ rs, Ord (F.Record ks), FI.RecVec binnedCols,  FI.RecVec binnedCols ,Real wt, 
                   x ∈ rs, x ~ (xl F.:-> a), KnownSymbol xl, Real a,
                   y ∈ rs, y ~ (yl F.:-> b), KnownSymbol yl, Real b,
                   w ∈ rs, w ~ (wl F.:-> wt), KnownSymbol wl,
                   binnedCols ~ (ks V.++ '[Bin2D, x, y, w]), Bin2D ∈ binnedCols, x ∈ binnedCols, y ∈ binnedCols, w ∈ binnedCols,
                   useCols ~ (ks V.++ '[x,y,w]), useCols F.⊆ rs, ks F.⊆ useCols, x ∈ useCols, y ∈ useCols, w ∈ useCols,
                   outKeyCols ~ (ks V.++ '[Bin2D]), outKeyCols F.⊆ binnedCols, Ord (F.Record outKeyCols),
                   (outKeyCols V.++ '[x,y,w]) ~ binnedCols)
             => Proxy ks
             -> Proxy '[x,y,w]
             -> (Double -> a) -- when we put the averaged data back in the record with original types we need to convert back
             -> (Double -> b)
             -> M.Map (F.Record ks) [a]
             -> M.Map (F.Record ks) [b]
             -> FL.Fold (F.Record rs) (F.FrameRec binnedCols)
scatterMerge _ _ toX toY xBins yBins =
  let xBinF = sortedListToBinLookup' <$> xBins
      yBinF = sortedListToBinLookup' <$> yBins
      trimRow :: F.Record rs -> F.Record useCols = F.rcast
      binRow :: F.Record useCols -> F.Record binnedCols -- 'ks ++ [Bin2D,x,y,w]
      binRow r =
        let key :: F.Record ks = F.rcast r
            xyw :: F.Record '[x,y,w] = F.rcast r
            xBF = fromMaybe (const 0) $ M.lookup key xBinF
            yBF = fromMaybe (const 0) $ M.lookup key yBinF
            bin :: F.Record '[Bin2D] = V.runcurryX (\x y _ -> (Bin2D (xBF x, yBF y) &: V.RNil)) xyw
        in key V.<+> bin V.<+> xyw 
      wgtdSum :: (Double, Double, wt) -> F.Record binnedCols -> (Double, Double, wt)
      wgtdSum (wX, wY, totW) r =
        let xyw :: F.Record '[x,y,w] = F.rcast r
        in  V.runcurryX (\x y w -> let w' = realToFrac w in (wX + (w' * realToFrac x), wY + (w' * realToFrac y), totW + w)) xyw
      extract :: [F.Record binnedCols] -> F.Record '[x,y,w]  
      extract = FL.fold (FL.Fold wgtdSum (0, 0, 0) (\(wX, wY, totW) -> let totW' = realToFrac totW in toX (wX/totW') &:  toY (wY/totW') &: totW &: V.RNil))
  in aggregateF (Proxy @outKeyCols) (V.Identity . binRow . trimRow) (:) [] extract 
      
  
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
