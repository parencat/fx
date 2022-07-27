(ns fx.utils.types
  (:require
   [malli.core :as m])
  (:import
   [java.sql Connection]))


(def connection?
  (m/-simple-schema
   {:type :db/connection
    :pred #(instance? Connection %)}))
