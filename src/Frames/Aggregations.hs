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
import           GHC.TypeLits (KnownSymbol)

data Bin2DT = Bin2D (Int, Int) deriving (Show, Eq, Ord)

F.declareColumn "Bin2D" ''Bin2DT

type instance FI.VectorFor Bin2DT = V.Vector
instance F.ShowCSV Bin2DT where
  showCSV = T.pack . show

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
type UseColsC ks x y w = (ks F.⊆ UseCols ks x y w, x ∈ UseCols ks x y w, y ∈ UseCols ks x y w, w ∈ UseCols ks x y w)
type OutKeyCols ks = ks V.++ '[Bin2D]
type BinnedDblCols ks w = ks V.++ '[Bin2D, X, Y, w]
type BinnedDblColsC ks w = (Bin2D ∈ BinnedDblCols ks w, X ∈ BinnedDblCols ks w, Y ∈ BinnedDblCols ks w, w ∈ BinnedDblCols ks w)
type BinnedResultCols ks x y w = ks V.++ '[Bin2D, x, y, w]
type DataField x = (V.KnownField x, Real (FType x))
-- This thing is...unfortunate. Is there something built into Frames or Vinyl that would do this?
class (DataField x, x ∈ rs) => DataFieldOf rs x
instance (DataField x, x ∈ rs) => DataFieldOf rs x

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
                      
  
