(ns fx.utils.types
  (:require
   [malli.core :as m])
  (:import
   [javax.sql DataSource]
   [java.time Clock]
   [org.postgresql.util PGobject]))


(def connection?
  (m/-simple-schema
   {:type :db/connection
    :pred #(instance? DataSource %)}))


(def clock?
  (m/-simple-schema
   {:type :java.time/clock
    :pred #(instance? Clock %)}))


(def pgobject?
  (m/-simple-schema
   {:type :pg/object
    :pred #(instance? PGobject %)}))
