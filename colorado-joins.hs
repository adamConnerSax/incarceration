{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
module Main where

-- my stuff
import           DataSources
import           Frames.Aggregations     as FA

--
import qualified Control.Foldl           as FL
import           Control.Lens            ((^.))
import           Control.Monad.IO.Class  (MonadIO, liftIO)
import qualified Data.List               as List
import qualified Data.Map                as M
import           Data.Maybe              (catMaybes, fromMaybe, isJust)
import           Data.Proxy              (Proxy (..))
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Vector             as V
import qualified Data.Vinyl              as V
import qualified Data.Vinyl.Class.Method as V
import           Data.Vinyl.Curry        (runcurryX)
import           Data.Vinyl.XRec         as V
import           Frames                  ((:.), (&:))
import qualified Frames                  as F
import qualified Frames.CSV              as F
import qualified Frames.InCore           as FI
import qualified Frames.ShowCSV          as F
import qualified Pipes                   as P
import qualified Pipes.Prelude           as P




