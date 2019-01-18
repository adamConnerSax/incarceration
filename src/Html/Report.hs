{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings    #-}
module Html.Report
  (
    makeReportHtmlAsText
  )
where

import           Data.Monoid    ((<>))
import qualified Data.Text      as T
import qualified Data.Text.Lazy as LT
import qualified Lucid          as H

makeReportHtmlAsText :: Monad m => T.Text -> H.HtmlT m a -> m LT.Text
makeReportHtmlAsText title body =
  H.renderTextT $ H.html_ (do
                              (H.head_ (do
                                           H.title_ (H.toHtmlRaw title)
                                           H.script_ [H.src_ ("https://cdn.jsdelivr.net/npm/vega@4.4.0" :: T.Text)]
                                           H.script_ [H.src_ ("https://cdn.jsdelivr.net/npm/vega-lite@3.0.0-rc11" :: T.Text)]
                                           H.script_ [H.src_ ("https://cdn.jsdelivr.net/npm/vega-embed@3.28.0" :: T.Text)]
                                       )
                                )
                              body
                          )
{-
                                         let vegalite_scripts = do
        H.script_ [H.src_ ("https://cdn.jsdelivr.net/npm/vega@4.4.0" :: T.Text)]
        H.script_ [H.src_ ("https://cdn.jsdelivr.net/npm/vega-lite@3.0.0-rc11" :: T.Text)]
        H.script_ [H.src_ ("https://cdn.jsdelivr.net/npm/vega-embed@3.28.0" :: T.Text)]
      head = H.head_ (do
                         H.title_ title
                         vegalite_scripts
                     )
  in H.renderTextT $ H.html_ (do
                                 head
                                 body
                             )
-}
