(ns fx.utils.types
  (:require
   [malli.core :as m])
  (:import
   [java.sql Connection]
   [java.time Clock]
   [org.postgresql.util PGobject]))


(def connection?
  (m/-simple-schema
   {:type :db/connection
    :pred #(instance? Connection %)}))


(def clock?
  (m/-simple-schema
   {:type :java.time/clock
    :pred #(instance? Clock %)}))


(def pgobject?
  (m/-simple-schema
   {:type :pg/object
    :pred #(instance? PGobject %)}))
