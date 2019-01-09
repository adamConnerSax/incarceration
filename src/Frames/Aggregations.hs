{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Frames.Aggregations
  (
    RescaleType (..)
  , BinsWithRescale (..)
  , goodDataCount
  , goodDataByKey
  , filterField
  , filterMaybeField
  , aggregateToMap
  , aggregateGeneral
  , aggregateFiltered
  , aggregateF
  , Binnable (..)
  , binField
  , rescale
  , ScatterMergeable (..)
  , scatterMerge
  , kMeans
  , euclidSq
  , scatterMergeOne
  , scatterMerge'
  , buildScatterMerge
  , reshapeRowSimple
  ) where

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import qualified Data.Foldable     as Foldable
import qualified Data.List            as List
import qualified Data.Map             as M
import           Data.Maybe           (fromMaybe, isJust, catMaybes, fromJust)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.XRec as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Core      as V
import           Data.Vinyl.Lens      (type (∈))
import           Data.Kind            (Type,Constraint)
import           Data.Profunctor      as PF
import           Frames               ((:.), (&:))
import qualified Frames               as F
import qualified Frames.CSV           as F
import qualified Frames.ShowCSV           as F
import qualified Frames.Melt           as F
import qualified Frames.InCore        as FI
import qualified Pipes                as P
import qualified Pipes.Prelude        as P
import           Control.Arrow (second)
import           Data.Proxy (Proxy(..))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import           GHC.TypeLits (KnownSymbol)
import           Data.Random.Source.PureMT as R
import           Data.Random as R
import           Control.Monad.State (evalState)
import           Data.Function (on)

goodDataByKey :: forall ks rs. (ks F.⊆ rs, Ord (F.Record ks)) => Proxy ks ->  FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (M.Map (F.Record ks) (Int, Int))
goodDataByKey _ =
  let getKey = F.recMaybe . F.rcast @ks
  in FL.prefilter (isJust . getKey) $ FL.Fold (aggregateToMap (fromJust . getKey) (flip (:)) []) M.empty (fmap $ FL.fold goodDataCount)   

goodDataCount :: FL.Fold (F.Rec (Maybe F.:. F.ElField) rs) (Int, Int)
goodDataCount = (,) <$> FL.length <*> FL.prefilter (isJust . F.recMaybe) FL.length

filterMaybeField :: forall k rs. (F.ElemOf rs k, Eq (V.HKD F.ElField k), (V.IsoHKD F.ElField k))
                 => Proxy k -> V.HKD F.ElField k -> F.Rec (Maybe :. F.ElField) rs -> Bool
filterMaybeField _ kv =
  let maybeTest t = maybe False t
  in maybeTest (== kv) . V.toHKD . F.rget @k

filterField :: forall k rs. (F.ElemOf rs k, Eq (V.HKD F.ElField k), (V.IsoHKD F.ElField k))
                 => Proxy k -> V.HKD F.ElField k -> F.Record rs -> Bool
filterField _ kv = (== kv) . V.toHKD . F.rget @k


aggregateToMap :: Ord k => (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> a -> M.Map k b
aggregateToMap getKey combine initial m r =
  let key = getKey r
      newVal = Just . flip combine r . fromMaybe initial 
  in M.alter newVal key m --M.insert key newVal m 

-- fold over c.  But c may become zero or many a (bad data, or melting rows). So we process c, then fold over the result.
aggregateGeneral :: (Ord k, Foldable f) => (c -> f a) -> (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateGeneral unpack getKey combine initial m x =
  let aggregate = FL.Fold (aggregateToMap getKey combine initial) m id
  in FL.fold aggregate (unpack x)

-- Maybe is delightfully foldable!  
aggregateFiltered :: Ord k => (c -> Maybe a) -> (a -> k) -> (b -> a -> b) -> b -> M.Map k b -> c -> M.Map k b
aggregateFiltered = aggregateGeneral

-- specific version for our record folds via Control.Foldl
aggregateFs :: forall rs ks as b cs f g h. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f, Foldable h, Functor h)
           => Proxy ks
           -> (F.Rec g rs -> f (F.Record as))
           -> (b -> F.Record as -> b)
           -> b
           -> (b -> h (F.Record cs))
           -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateFs _ unpack process initial extract =
  let addKey :: (F.Record ks, h (F.Record cs)) -> h (F.Record (ks V.++ cs))
      addKey (k, hcs) = fmap (V.rappend k) hcs
  in FL.Fold (aggregateGeneral unpack (F.rcast @ks) process initial) M.empty (F.toFrame . List.concat . fmap (Foldable.toList . addKey . second extract) . M.toList) 

aggregateF :: forall rs ks as b cs f g. (ks F.⊆ as, Ord (F.Record ks), FI.RecVec (ks V.++ cs), Foldable f)
           => Proxy ks
           -> (F.Rec g rs -> f (F.Record as))
           -> (b -> F.Record as -> b)
           -> b
           -> (b -> F.Record cs)
           -> FL.Fold (F.Rec g rs) (F.FrameRec (ks V.++ cs))
aggregateF pks unpack process initial extract = aggregateFs pks unpack process initial (V.Identity . extract)

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

-- We use a GADT here so that each constructor can carry the proofs of numerical type.  E.g., We don't want RescaleNone to require RealFloat. 
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

type FType x = V.Snd x

type Binnable x w =  (V.KnownField x, Num (FType x), Ord (FType x),
                      V.KnownField w, Real (FType w), Num (FType w))
                     
binField :: forall x w. Binnable x w => Int -> RescaleType (FType x) -> FL.Fold (F.Record '[x,w]) (BinsWithRescale (FType x))
binField numBins rt =
  let process :: [(FType x, FType w)] -> F.Record '[x,w] -> [(FType x, FType w)]  
      process l r  = V.runcurryX (\x w -> (x, w) : l) r
      extract :: RescaleType (FType x) -> [(FType x, FType w)] -> BinsWithRescale (FType x)
      extract rt l =
        let compFst (x,_) (x',_) = compare x x'
            scaleInfo = rescale rt (fst <$> l)
            (totalWeight, listWithSummedWeights) = List.mapAccumL (\sw (x,w) -> (sw+w, (x,w,sw+w))) 0 $ List.sortBy compFst l
            weightPerBin :: Double = (realToFrac totalWeight)/(fromIntegral numBins)
            lowerBounds :: [(b, FType w, FType w)] -> [b] -> [b]
            lowerBounds x bs = case List.null x of
              True -> bs
              False ->
                let nextLB = (\(x,_,_) -> x) . head $ x
                    newX = List.dropWhile (\(_,_,sw) -> (realToFrac sw) < weightPerBin * (fromIntegral $ List.length bs + 1)) x
                    newBS = bs ++ [nextLB]
                in lowerBounds newX newBS
        in BinsWithRescale (lowerBounds listWithSummedWeights []) (fst scaleInfo) (snd scaleInfo)
  in FL.Fold process [] (extract rt)

type KeyedRecord ks rs = (ks F.⊆ rs, Ord (F.Record ks))
type BinnableKeyedRecord rs ks x w = (Binnable x w, KeyedRecord ks rs, '[x,w] F.⊆ rs)
  
binFields :: forall rs ks x w. BinnableKeyedRecord rs ks x w
           => Int -> Proxy ks -> Proxy '[x,w] -> RescaleType (FType x) -> FL.Fold (F.Record rs) (M.Map (F.Record ks) (BinsWithRescale (FType x)))
binFields n _ _ rt =
  let unpack = V.Identity
      combine :: [F.Record '[x,w]] -> F.Record rs -> [F.Record '[x,w]]
      combine l r = F.rcast r : l
  in FL.Fold (aggregateGeneral unpack (F.rcast @ks) combine []) M.empty (fmap (FL.fold (binField n rt)))

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

type X = "x" F.:-> Double
type Y = "y" F.:-> Double

type UseCols ks x y w = ks V.++ '[x,y,w]
type DataField x = (V.KnownField x, Real (FType x))

-- This thing is...unfortunate. Is there something built into Frames or Vinyl that would do this?
class (DataField x, x ∈ rs) => DataFieldOf rs x
instance (DataField x, x ∈ rs) => DataFieldOf rs x

type ScatterMergeable rs ks x y w = (ScatterMergeableData x y w, FI.RecVec (ks V.++ [x,y,w]),
                                     F.AllConstrained (DataFieldOf [x,y,w]) '[x, y, w], KeyedRecord ks rs,
                                     ks F.⊆ (ks V.++ [x,y,w]), (ks V.++ [x,y,w]) F.⊆ rs,
                                     F.ElemOf (ks V.++ [x,y,w]) x, F.ElemOf (ks V.++ [x,y,w]) y, F.ElemOf (ks V.++ [x,y,w]) w)
         
scatterMerge :: forall rs ks x y w. ScatterMergeable rs ks x y w
              => Proxy ks
              -> Proxy '[x,y,w]
              -> (Double -> FType x) -- when we put the averaged data back in the record with original types we need to convert back
              -> (Double -> FType y)
              -> Int
              -> Int
              -> RescaleType (FType x)
              -> RescaleType (FType y)
              -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
scatterMerge proxy_ks _ toX toY numBinsX numBinsY rtX rtY =
  let combine l r = F.rcast @[x,y,w] r : l
      toRecord :: (Double, Double, FType w) -> F.Record '[x,y,w]
      toRecord (x', y', w) = toX x' &: toY y' &: w &: V.RNil 
  in aggregateFs proxy_ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] (fmap toRecord . scatterMergeOne numBinsX numBinsY rtX rtY)

type ScatterMergeableData x y w = (Binnable x w, Binnable y w, [x,w] F.⊆ [x,y,w], [y,w] F.⊆ [x,y,w], Real (FType x), Real (FType y))
  
scatterMergeOne :: forall x y w f. (ScatterMergeableData x y w, Foldable f)
                => Int -> Int -> RescaleType (FType x) -> RescaleType (FType y) -> f (F.Record '[x,y,w]) -> [(Double, Double, FType w)]
scatterMergeOne numBinsX numBinsY rtX rtY dataRows =
  let xBinF = PF.lmap (F.rcast @[x,w]) $ binField numBinsX rtX
      yBinF = PF.lmap (F.rcast @[y,w]) $ binField numBinsY rtY
      (xBins, yBins) = FL.fold ((,) <$> xBinF <*> yBinF) dataRows
      binningInfo (BinsWithRescale bs shift scale) = (sortedListToBinLookup' bs, (\x -> realToFrac (x - shift)/scale))
      (binX, scaleX) = binningInfo xBins
      (binY, scaleY) = binningInfo yBins
      binAndScale :: F.Record '[x,y,w] -> ((Int, Int), Double, Double, FType w)
      binAndScale = V.runcurryX (\x y w -> ((binX x, binY y),scaleX x, scaleY y, w))
      getKey (k,_,_,_) = k
      getData (_,x,y,w) = (x,y,w)
      combine l x = getData x : l
      wgtdSumF :: FL.Fold (Double, Double, FType w) (Double, Double, FType w)
      wgtdSumF =
        let f (wX, wY, totW) (x, y, w) = let w' = realToFrac w in (wX + w' * x, wY + w' * y, totW + w)
        in FL.Fold f (0, 0 , 0) (\(wX, wY, totW) -> let tw = realToFrac totW in (wX/tw, wY/tw, totW))
  in FL.fold (FL.Fold (aggregateGeneral (V.Identity . binAndScale) getKey combine []) M.empty (fmap (FL.fold wgtdSumF . snd) . M.toList)) dataRows
        

-- k-means
-- use the weights when computing the centroid location
-- initialize with random centers (Forgy) for now.

data Weighted a w = Weighted { dimension :: Int, location :: a -> U.Vector Double, weight :: a -> w } 
newtype Cluster a = Cluster { members :: [a] } deriving (Eq, Show)
newtype Clusters a = Clusters (V.Vector (Cluster a))  deriving (Show, Eq)
newtype Centroids = Centroids { centers :: V.Vector (U.Vector Double) } deriving (Show)
type Distance = U.Vector Double -> U.Vector Double -> Double

emptyCluster :: Cluster a 
emptyCluster = Cluster []

-- compute some initial random locations
-- TODO: use normal dist around mean and std-dev
forgyCentroids :: forall x y w f. (F.AllConstrained (DataFieldOf [x,y,w]) '[x, y, w], Foldable f)
               => Int -- number of clusters
               -> R.PureMT
               -> f (F.Record '[x,y,w])
               -> ([(Double, Double)], R.PureMT) 
forgyCentroids n randomSource dataRows =
  let h = fromMaybe (0 :: Double) . fmap realToFrac   
      (xMin, xMax, yMin, yMax) = FL.fold ((,,,)
                                           <$> PF.dimap (F.rgetField @x) h FL.minimum
                                           <*> PF.dimap (F.rgetField @x) h FL.maximum
                                           <*> PF.dimap (F.rgetField @y) h FL.minimum
                                           <*> PF.dimap (F.rgetField @y) h FL.maximum) dataRows
      uniformPair = do
        ux <- R.uniform xMin xMax -- TODO: this is not a good way to deal with the Maybe here
        uy <- R.uniform yMin yMax
        return (ux,uy)
      uniformPairList :: R.RVar [(Double, Double)] = mapM (const uniformPair) $ replicate n ()
  in R.sampleState uniformPairList randomSource

weighted2DRecord :: forall x y w. (F.AllConstrained (DataFieldOf [x,y,w]) '[x, y, w]) => Weighted (F.Record '[x,y,w]) (FType w)
weighted2DRecord = Weighted 2 (V.runcurryX (\x y _ -> U.fromList [realToFrac x, realToFrac y])) (F.rgetField @w)

-- compute the centroid of the data.  Useful for the kMeans algo and computing the centroid of the final clusters
centroid :: forall g w a. (Foldable g, Real w) => Weighted a w -> g a -> (U.Vector Double, w)
centroid weighted as =
  let addOne :: (U.Vector Double, w) -> a -> (U.Vector Double, w)
      addOne (sumV, sumW) x =
        let w = weight weighted $ x
            v = location weighted $ x
        in (U.zipWith (+) sumV (U.map (* (realToFrac w)) v), sumW + w)
      finishOne (sumV, sumW) = U.map (/(realToFrac sumW)) sumV
  in FL.fold ((,) <$> FL.Fold addOne (U.replicate (dimension weighted) 0, 0) finishOne <*> FL.premap (weight weighted) FL.sum) as

euclidSq :: Distance
euclidSq v1 v2 = U.sum $ U.zipWith diffsq v1 v2
  where diffsq a b = (a-b)^(2::Int)

-- this is the same as ScatterMergable so perhaps should be renamed to ?
type KMeansable rs ks x y w = (ScatterMergeableData x y w, FI.RecVec (ks V.++ [x,y,w]),
                               F.AllConstrained (DataFieldOf [x,y,w]) '[x, y, w], KeyedRecord ks rs,
                               ks F.⊆ (ks V.++ [x,y,w]), (ks V.++ [x,y,w]) F.⊆ rs,
                               F.ElemOf (ks V.++ [x,y,w]) x, F.ElemOf (ks V.++ [x,y,w]) y, F.ElemOf (ks V.++ [x,y,w]) w)
-- TODOS:
-- Add rescaling
-- Thread random source through
kMeans :: forall rs ks x y w. (KMeansable rs ks x y w)
       => Proxy ks
       -> Proxy '[x,y,w]
       -> (Double -> FType x) -- when we put the averaged data back in the record with original types we need to convert back
       -> (Double -> FType y)
       -> Int 
       -> Distance
       -> PureMT
       -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
kMeans proxy_ks _ toX toY numClusters  distance randomSource =
  let combine l r = F.rcast @[x,y,w] r : l
      toRecord :: (Double, Double, FType w) -> F.Record '[x,y,w]
      toRecord (x', y', w) = toX x' &: toY y' &: w &: V.RNil
      computeOne rows = kMeansOne initial weighted2DRecord distance rows where
        initial = fst $ forgyCentroids numClusters randomSource rows
  in aggregateFs proxy_ks (V.Identity . (F.rcast @(ks V.++ [x,y,w]))) combine [] (fmap toRecord . computeOne)


kMeansOne :: forall x y w f. (Foldable f, F.AllConstrained (DataFieldOf [x,y,w]) '[x,y,w])
          => [(Double, Double)]  -- initial centroids
          -> Weighted (F.Record '[x,y,w]) (FType w) 
          -> Distance
          -> f (F.Record '[x,y,w])
          -> [(Double, Double, FType w)]
kMeansOne initial weighted distance dataRows =
  let initialCentroids = Centroids $ V.fromList $ fmap (\(x,y) -> U.fromList [x,y]) initial
      (Clusters clusters) = weightedKMeans initialCentroids weighted distance dataRows
      fix :: (U.Vector Double, FType w) -> (Double, Double, FType w)
      fix (v, wgt) = (v U.! 0, v U.! 1, wgt)
  in V.toList $ fmap (fix . centroid weighted . members) clusters
        
weightedKMeans :: forall a w f. (Foldable f, Real w, Eq a)
               => Centroids -- initial guesses at centers 
               -> Weighted a w -- location/weight from data
               -> Distance
               -> f a
               -> Clusters a 
weightedKMeans initial weighted distF as = 
  -- put them all in one cluster just to start.  Doesn't matter since we have initial centroids
  let k = V.length (centers initial) -- number of clusters
      d = U.length (V.head $ centers initial) -- number of dimensions
      clusters0 = Clusters (V.fromList $ Cluster (Foldable.toList as) : (List.replicate (k - 1) emptyCluster))
      nearest :: Centroids -> a -> Int
      nearest cs a = V.minIndexBy (compare `on` distF (location weighted a)) (centers cs)
      updateClusters :: Centroids -> Clusters a -> Clusters a
      updateClusters centroids (Clusters oldCs) =
        let doOne :: [(Int, a)] -> Cluster a -> [(Int, a)]
            doOne soFar (Cluster as) = FL.fold (FL.Fold (\l x -> (nearest centroids x, x) : l) soFar id) as
            repackage :: [(Int, a)] -> Clusters a
            repackage = Clusters . V.unsafeAccum (\(Cluster l) x -> Cluster (x : l)) (V.replicate k emptyCluster)
        in FL.fold (FL.Fold doOne [] repackage) oldCs
      centroids :: Clusters a -> Centroids
      centroids (Clusters cs) = Centroids $ fmap (fst. centroid weighted . members) cs
      doStep cents clusters = let newClusters = updateClusters cents clusters in (newClusters, centroids newClusters) 
      go oldClusters (newClusters, centroids) =
        case (oldClusters == newClusters) of
          True -> newClusters
          False -> go newClusters (doStep centroids newClusters)
  in go clusters0 (doStep initial clusters0)

-- All unused below but might be useful to have around.

data Bin2DT = Bin2D (Int, Int) deriving (Show, Eq, Ord)

F.declareColumn "Bin2D" ''Bin2DT

type instance FI.VectorFor Bin2DT = V.Vector
instance F.ShowCSV Bin2DT where
  showCSV = T.pack . show


type OutKeyCols ks = ks V.++ '[Bin2D]
type BinnedDblCols ks w = ks V.++ '[Bin2D, X, Y, w]
type BinnedDblColsC ks w = (Bin2D ∈ BinnedDblCols ks w, X ∈ BinnedDblCols ks w, Y ∈ BinnedDblCols ks w, w ∈ BinnedDblCols ks w)
type BinnedResultCols ks x y w = ks V.++ '[Bin2D, x, y, w]
type UseColsC ks x y w = (ks F.⊆ UseCols ks x y w, x ∈ UseCols ks x y w, y ∈ UseCols ks x y w, w ∈ UseCols ks x y w)

type ScatterMergeable' rs ks x y w = (ks F.⊆ rs,
                                      Ord (F.Record ks),
                                      FI.RecVec (BinnedResultCols ks x y w),
                                       F.AllConstrained (DataFieldOf rs) '[x, y, w],
                                       BinnedDblColsC ks w,
                                       UseCols ks x y w F.⊆ rs, UseColsC ks x y w,
                                       OutKeyCols ks F.⊆ BinnedDblCols ks w,
                                       Ord (F.Record (OutKeyCols ks)),
                                       UseCols ks x y w F.⊆ BinnedResultCols ks x y w,
                                       ((OutKeyCols ks) V.++ '[x,y,w]) ~ (BinnedResultCols ks x y w))

scatterMerge' :: forall rs ks x y w. ScatterMergeable' rs ks x y w
             => Proxy ks
             -> Proxy '[x,y,w]
             -> (Double -> FType x) -- when we put the averaged data back in the record with original types we need to convert back
             -> (Double -> FType y)
             -> M.Map (F.Record ks) (BinsWithRescale (FType x))
             -> M.Map (F.Record ks) (BinsWithRescale (FType y))
             -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
scatterMerge' _ _ toX toY xBins yBins =
  let binningInfo :: Real c => BinsWithRescale c -> (c -> Int, c -> Double)
      binningInfo (BinsWithRescale bs shift scale) = (sortedListToBinLookup' bs, (\x -> realToFrac (x - shift)/scale))
      xBinF = binningInfo <$> xBins
      yBinF = binningInfo <$> yBins
      binRow :: F.Record (UseCols ks x y w) -> F.Record (BinnedDblCols ks w) -- 'ks ++ [Bin2D,X,Y,w]
      binRow r =
        let key = F.rcast @ks r
            xyw = F.rcast @[x,y,w] r
            (xBF, xSF) = fromMaybe (const 0, realToFrac) $ M.lookup key xBinF
            (yBF, ySF) = fromMaybe (const 0, realToFrac) $ M.lookup key yBinF
            binnedAndScaled :: F.Record '[Bin2D, X, Y, w] = V.runcurryX (\x y w -> Bin2D (xBF x, yBF y) &: xSF x &: ySF y &: w &: V.RNil) xyw
        in key V.<+> binnedAndScaled 
      wgtdSum :: (Double, Double, FType w) -> F.Record (BinnedDblCols ks w) -> (Double, Double, FType w)
      wgtdSum (wX, wY, totW) r =
        let xyw :: F.Record '[X,Y,w] = F.rcast r
        in  V.runcurryX (\x y w -> let w' = realToFrac w in (wX + (w' * x), wY + (w' * y), totW + w)) xyw
      extract :: [F.Record (BinnedDblCols ks w)] -> F.Record '[x,y,w]  
      extract = FL.fold (FL.Fold wgtdSum (0, 0, 0) (\(wX, wY, totW) -> let totW' = realToFrac totW in toX (wX/totW') &:  toY (wY/totW') &: totW &: V.RNil))
  in fmap (fmap (F.rcast @(UseCols ks x y w))) $ aggregateF (Proxy @(OutKeyCols ks)) (V.Identity . binRow . (F.rcast @(UseCols ks x y w))) (\l a -> a : l) [] extract     


type BinMap ks x = M.Map (F.Record ks) (BinsWithRescale (FType x))
buildScatterMerge :: forall rs ks x y w. (BinnableKeyedRecord rs ks x w, BinnableKeyedRecord rs ks y w, ScatterMergeable' rs ks x y w)
                  => Proxy ks
                  -> Proxy '[x,y,w]
                  -> Int
                  -> Int
                  -> RescaleType (FType x)
                  -> RescaleType (FType y)
                  -> (Double -> FType x)
                  -> (Double -> FType y)
                  -> (FL.Fold (F.Record rs) (BinMap ks x, BinMap ks y),
                      (BinMap ks x, BinMap ks y) -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w)))
buildScatterMerge proxy_ks proxy_xyw xNumBins yNumBins rtX rtY toX toY =
  let binXFold = binFields xNumBins proxy_ks (Proxy @[x,w]) rtX
      binYFold = binFields yNumBins proxy_ks (Proxy @[y,w]) rtY
      smFold (xBins, yBins) = scatterMerge' proxy_ks proxy_xyw toX toY xBins yBins
  in ((,) <$> binXFold <*> binYFold, smFold)
                      
  
