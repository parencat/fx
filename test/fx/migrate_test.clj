(ns fx.migrate-test
  (:require
   [clojure.test :refer :all]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [fx.containers.postgres :as pg]
   [honey.sql :as sql])
  (:import [java.sql DatabaseMetaData Types]))

