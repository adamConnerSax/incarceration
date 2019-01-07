{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module Frames.MaybeUtils where

import           Frames                  ((:.), (&:))
import qualified Frames                  as F
import qualified Frames.CSV              as F
import qualified Frames.ShowCSV          as F
import qualified Frames.Melt as F
import qualified Pipes                   as P
import qualified Data.Vinyl              as V
import qualified Data.Vinyl.Derived      as V
import qualified Data.Vinyl.TypeLevel      as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Core         as V
import           Data.Vinyl.Curry        (runcurryX)
import qualified Data.Vinyl.Functor      as V
import           Data.Proxy              (Proxy (..))
import Data.Maybe (fromMaybe, fromJust)


-- TBD: This should be an argument to produceCSV_Maybe and writeCSV_Maybe.
instance F.ShowCSV a => F.ShowCSV (Maybe a) where
  showCSV = fromMaybe "NA" . fmap F.showCSV

--type instance FI.VectorFor (Maybe a) = V.Vector

type AddMaybe rs = V.MapTyCon Maybe rs

{-
-- delete the first occurence of each of a list of types from a type-level list 
type family RDeleteL ds rs where
  RDeleteL '[] rs = rs
  RDeleteL (d ': ds) rs = RDeleteL ds (F.RDelete d rs)
-}

unMaybeKeys :: forall ks rs as.(ks F.⊆ rs, as ~ F.RDeleteAll ks rs, as F.⊆ rs, V.RPureConstrained V.KnownField as, V.RecApplicative as, V.RApply as, V.RMap as)
  => Proxy ks -> F.Rec (Maybe F.:. F.ElField) rs -> F.Record (ks V.++ (AddMaybe as))
unMaybeKeys _ mr =
  let keys = fromJust $ F.recMaybe $ F.rcast @ks mr
      remainder = V.rsequenceInFields $ F.rcast @as mr
 in keys `V.rappend` remainder

{-
leftJoinMaybe :: forall fs rs rs2  rs2' rs1'.
                 ( fs    F.⊆ rs
                 , fs   F.⊆ rs2
                 , rs F.⊆ (rs V.++ rs2')
                 , rs2' F.⊆ rs2
                 , rs2' ~ F.RDeleteAll fs rs2
                 , rs1' ~ F.RDeleteAll fs rs1
                 , V.RMap rs
                 , V.RMap (rs V.++ rs2')
                 , V.RecApplicative rs2'
                 , V.Grouping (V.Record fs)
                 , F.RecVec rs
                 , F.RecVec rs2'
                 , F.RecVec (rs V.++ rs2')
                 )
              => Proxy fs
              -> Frame (Rec (Maybe :. F.ElField) rs)  -- ^ The left frame
              -> Frame (Rec (Maybe :. F.ElField) rs2) -- ^ The right frame
              -> [Rec (Maybe :. ElField) (rs ++ rs2')] -- ^ A list of the merged records, now in the Maybe functor
leftJoinMaybe proxy_keys f1 f2 =
  let umF1 = unMaybeKeys proxy_keys f1
      umF2 = unMaybeKeys proxy_keys f2
      lj = F.leftJoin @fs umF1 umF2 -- [Rec (Maybe :. ElField) (fs ++ (AddMaybe rs1') ++ (AddMaybe rs2')) 
      ljKeys = F.rcast @fs lj
--      ljRemainder = F.rcast @
-}

-- These will need more constraints!!
produceCSV_Maybe :: forall f ts m.
                   (F.ColumnHeaders (AddMaybe ts), Foldable f, Functor f, Monad m, V.RecordToList (AddMaybe ts),
                    V.RPureConstrained V.KnownField ts, V.RecApplicative ts, V.RApply ts, V.RMap ts,
                    V.RecMapMethod F.ShowCSV F.ElField (AddMaybe ts)) => f (F.Rec (Maybe :. F.ElField) ts) -> P.Producer String m ()                  
produceCSV_Maybe = F.produceCSV . fmap V.rsequenceInFields


writeCSV_Maybe :: (F.ColumnHeaders (AddMaybe ts), Foldable f, Functor f, V.RecordToList (AddMaybe ts),
                   V.RPureConstrained V.KnownField ts, V.RecApplicative ts, V.RApply ts, V.RMap ts,
                   V.RecMapMethod F.ShowCSV F.ElField (AddMaybe ts))
              => FilePath -> f (F.Rec (Maybe :. F.ElField) ts) -> IO ()
writeCSV_Maybe fp = F.writeCSV fp . fmap V.rsequenceInFields
