{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings    #-}
module Html.Report
  (
    makeReportHtmlAsText
  , placeVisualization
  , placeTextSection
  )
where

import           Data.Monoid    ((<>))
import qualified Data.Text      as T
import qualified Data.Text.Encoding         as T
import qualified Data.ByteString.Lazy.Char8 as BS
import qualified Graphics.Vega.VegaLite     as GV
import qualified Data.Text.Lazy as LT
import qualified Lucid          as H
import qualified Data.Aeson.Encode.Pretty   as A


makeReportHtmlAsText :: forall m a. Monad m => T.Text -> H.HtmlT m a -> m LT.Text
makeReportHtmlAsText title body = 
  let html :: H.HtmlT m a = H.html_ $ head >> body
      head :: H.HtmlT m () = H.head_ (do
                                         H.title_ (H.toHtmlRaw title)
                                         H.script_ [H.src_ "https://cdn.jsdelivr.net/npm/vega@4.4.0"] ""
                                         H.script_ [H.src_ "https://cdn.jsdelivr.net/npm/vega-lite@3.0.0-rc11"] ""
                                         H.script_ [H.src_ "https://cdn.jsdelivr.net/npm/vega-embed@3.28.0"] ""
                                         return ()
                                     )
  in H.renderTextT html
 

placeVisualization :: Monad m => T.Text -> GV.VegaLite -> H.HtmlT m ()  
placeVisualization idText vl =
  let vegaScript :: T.Text = T.decodeUtf8 $ BS.toStrict $ A.encodePretty $ GV.fromVL vl
      script = "var vlSpec=\n" <> vegaScript <> ";\n" <> "vegaEmbed(\'#" <> idText <> "\',vlSpec);"      
  in H.div_ [H.id_ idText] (H.script_ [H.type_ "text/javascript"]  (H.toHtmlRaw script))

placeTextSection :: Monad m => H.HtmlT m () -> H.HtmlT m ()
placeTextSection x = H.div_ [{- attributes/styles here -}] x
