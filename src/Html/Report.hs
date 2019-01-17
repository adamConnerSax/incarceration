{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
module Html.Report
  (
    HtmlT (..)
  , addSeqElement
  , makeReport
  )
where

import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Indexed.State    (IxStateT(..), imodify)
import qualified Data.Text              as T
import qualified Html                   as H
import qualified Html.Attribute         as HA
import Html.Type (--(#)
                 type (#) (..)
                 , (:@:)(..)
                 ,type (?>) (..)
                 ,type (<?>)(..)
                 , type (>) (..))

newtype HtmlT m i j a = HtmlT { unHtmlT :: IxStateT m i j a } 

-- do we need to constrain this to valid children?  How?
addSeqElement :: Monad m => b -> HtmlT m a (a # b) ()
addSeqElement e = HtmlT $ imodify (\s -> s H.# e)

addChild :: (('H.Element <?> b) c, Monad m) => b -> c -> HtmlT m H.Element (('H.Element :@: b) c) ()
addChild attrs c = HtmlT $ imodify (\s -> WithAttributes attrs c)  

--startReport :: Monad m => HtmlT m i j ()
--startReport = HtmlT (IxStateT (\initial -> 

makeReportG :: (Monad m, H.Html ?> a, H.Html ?> b) => a -> HtmlT m a (a # b) () -> m (H.Html > (a # b))
makeReportG init r = do
  (_,s) <- runIxStateT (unHtmlT r) init
  return $ H.html_ s

makeReport title r =
  let initial =
        H.head_
        (
          H.title_ title
          H.# H.script_A (HA.src_ ("https://cdn.jsdelivr.net/npm/vega@4.4.0" :: T.Text)) ()
          H.# H.script_A (HA.src_ ("https://cdn.jsdelivr.net/npm/vega-lite@3.0.0-rc11" :: T.Text)) ()
          H.# H.script_A (HA.src_ ("https://cdn.jsdelivr.net/npm/vega-embed@3.28.0" :: T.Text)) ()
        )
  in makeReportG initial r
