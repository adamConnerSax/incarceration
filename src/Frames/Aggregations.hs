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
module Frames.Aggregations
  (
    RescaleType (..)
  , BinsWithRescale (..)
  , aggregateGeneral
  , aggregateFiltered
  , aggregateF
  , Binnable (..)
  , binField
  , rescale
  , ScatterMergeable (..)
  , scatterMerge
  , buildScatterMerge
  , reshapeRowSimple
  ) where

import qualified Control.Foldl        as FL
import           Control.Lens         ((^.))
import qualified Control.Lens         as L
import qualified Data.List            as List
import qualified Data.Map             as M
import           Data.Maybe           (fromMaybe, isJust, catMaybes)
import           Data.Text            (Text)
import qualified Data.Text            as T
import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Curry     as V
import qualified Data.Vinyl.Functor   as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Core      as V
import           Data.Vinyl.Lens      (type (∈))
import           Data.Kind            (Type,Constraint)
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

type FType x = V.Snd x

type Binnable rs ks f w = (V.KnownField f, Num (FType f), Ord (FType f), F.ElemOf rs f,
                           V.KnownField w, Integral (FType w), Num (FType w), F.ElemOf rs w,
                           Ord (F.Record ks), ks F.⊆ rs)

binField :: forall rs ks f w. Binnable rs ks f w
         => Int -> Proxy ks -> Proxy '[f,w] -> RescaleType (FType f) -> FL.Fold (F.Record rs) (M.Map (F.Record ks) (BinsWithRescale (FType f)))
binField n _ _ rt =
  let getKey :: F.Record rs -> F.Record ks = F.rcast
      getFW :: F.Record rs -> F.Record '[f,w] = F.rcast
      process :: F.Record rs -> [(FType f, FType w)] -> [(FType f, FType w)]
      process r l = V.runcurryX (\f w -> (f,w) : l) $ getFW r
      extract :: RescaleType (FType f) -> [(FType f, FType w)] -> BinsWithRescale (FType f)
      extract rt l =
        let compFst (x,_) (x',_) = compare x x'
            scaleInfo = rescale rt (fst <$> l)
            (totalWeight, listWithSummedWeights) = List.mapAccumL (\sw (f,w) -> (sw+w, (f,w,sw+w))) 0 $ List.sortBy compFst l
            weightPerBin :: Double = fromIntegral totalWeight/fromIntegral n
            lowerBounds :: [(b, FType w, FType w)] -> [b] -> [b]
            lowerBounds x bs = case List.null x of
              True -> bs
              False ->
                let nextLB = (\(x,_,_) -> x) . head $ x
                    newX = List.dropWhile (\(_,_,sw) -> (fromIntegral sw) < weightPerBin * (fromIntegral $ List.length bs + 1)) x
                    newBS = bs ++ [nextLB]
                in lowerBounds newX newBS
        in BinsWithRescale (lowerBounds listWithSummedWeights []) (fst scaleInfo) (snd scaleInfo)
  in FL.Fold (aggregateGeneral V.Identity getKey process []) M.empty (fmap $ extract rt)

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

-- This thing is...unfortunate. Is there something built into Frames or Vinyl that would do this?
class (V.KnownField x, x ∈ rs, Real (FType x)) => DataFieldOf rs x
instance (V.KnownField x, x ∈ rs, Real (FType x)) => DataFieldOf rs x

type ScatterMergeable rs ks x y w = (ks F.⊆ rs,
                                     Ord (F.Record ks),
                                     FI.RecVec (BinnedResultCols ks x y w),
                                     F.AllConstrained (DataFieldOf rs) '[x, y, w],
                                     BinnedDblColsC ks w,
                                     UseCols ks x y w F.⊆ rs, UseColsC ks x y w,
                                     OutKeyCols ks F.⊆ BinnedDblCols ks w,
                                     Ord (F.Record (OutKeyCols ks)),
                                     UseCols ks x y w F.⊆ BinnedResultCols ks x y w,
                                     ((OutKeyCols ks) V.++ '[x,y,w]) ~ (BinnedResultCols ks x y w))

scatterMerge :: forall rs ks x y w. ScatterMergeable rs ks x y w
             => Proxy ks
             -> Proxy '[x,y,w]
             -> (Double -> FType x) -- when we put the averaged data back in the record with original types we need to convert back
             -> (Double -> FType y)
             -> M.Map (F.Record ks) (BinsWithRescale (FType x))
             -> M.Map (F.Record ks) (BinsWithRescale (FType y))
             -> FL.Fold (F.Record rs) (F.FrameRec (UseCols ks x y w))
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
      wgtdSum :: (Double, Double, FType w) -> F.Record (BinnedDblCols ks w) -> (Double, Double, FType w)
      wgtdSum (wX, wY, totW) r =
        let xyw :: F.Record '[X,Y,w] = F.rcast r
        in  V.runcurryX (\x y w -> let w' = realToFrac w in (wX + (w' * x), wY + (w' * y), totW + w)) xyw
      extract :: [F.Record (BinnedDblCols ks w)] -> F.Record '[x,y,w]  
      extract = FL.fold (FL.Fold wgtdSum (0, 0, 0) (\(wX, wY, totW) -> let totW' = realToFrac totW in toX (wX/totW') &:  toY (wY/totW') &: totW &: V.RNil))
      removeBin2D :: F.Record (BinnedResultCols ks x y w) -> F.Record (UseCols ks x y w) = F.rcast
  in fmap (fmap removeBin2D) $ aggregateF (Proxy @(OutKeyCols ks)) (V.Identity . binRow . trimRow) (:) [] extract 
      

type BinMap ks x = M.Map (F.Record ks) (BinsWithRescale (FType x))
buildScatterMerge :: forall rs ks x y w. (Binnable rs ks x w, Binnable rs ks y w, ScatterMergeable rs ks x y w)
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
  let binXFold = binField xNumBins proxy_ks (Proxy @[x,w]) rtX
      binYFold = binField yNumBins proxy_ks (Proxy @[y,w]) rtY
      smFold (xBins, yBins) = scatterMerge proxy_ks proxy_xyw toX toY xBins yBins
  in ((,) <$> binXFold <*> binYFold, smFold)
                      
  
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

