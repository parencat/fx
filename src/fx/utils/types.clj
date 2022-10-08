(ns fx.utils.types
  (:refer-clojure :exclude [bigint double char time])
  (:require
   [malli.core :as m])
  (:import
   [javax.sql DataSource]
   [java.time Clock LocalDateTime OffsetDateTime LocalDate LocalTime OffsetTime Duration]
   [org.postgresql.util PGobject]
   [java.sql Connection]))


(def connection?
  (m/-simple-schema
   {:type :db/connection
    :pred #(or (instance? DataSource %)
               (instance? Connection %))}))


(def clock?
  (m/-simple-schema
   {:type :java.time/clock
    :pred #(instance? Clock %)}))


(def pgobject?
  (m/-simple-schema
   {:type :pg/object
    :pred #(instance? PGobject %)}))


(def smallint
  (m/-simple-schema
   {:type            :smallint
    :pred            int?
    :type-properties {:error/message "should be an instance of java.lang.Short"}}))


(def bigint
  (m/-simple-schema
   {:type            :bigint
    :pred            #(instance? Long %)
    :type-properties {:error/message "should be an instance of java.lang.Long"}}))


(def integer
  (m/-simple-schema
   {:type            :integer
    :pred            int?
    :type-properties {:error/message "should be a fixed precision integer"}}))


(def decimal
  (m/-simple-schema
   {:type            :decimal
    :pred            double?
    :type-properties {:error/message "should be an instance of java.lang.Double"}}))


(def numeric
  (m/-simple-schema
   {:type            :numeric
    :pred            double?
    :type-properties {:error/message "should be an instance of java.lang.Double"}}))


(def real
  (m/-simple-schema
   {:type            :real
    :pred            float?
    :type-properties {:error/message "should be an instance of java.lang.Float"}}))


(def double
  (m/-simple-schema
   {:type            :double
    :pred            double?
    :type-properties {:error/message "should be an instance of java.lang.Double"}}))


(def smallserial
  (m/-simple-schema
   {:type            :smallserial
    :pred            int?
    :type-properties {:error/message "should be an instance of java.lang.Short"}}))


(def serial
  (m/-simple-schema
   {:type            :serial
    :pred            int?
    :type-properties {:error/message "should be a fixed precision integer"}}))


(def bigserial
  (m/-simple-schema
   {:type            :bigserial
    :pred            #(instance? Long %)
    :type-properties {:error/message "should be an instance of java.lang.Long"}}))


(def char
  (m/-simple-schema
   {:type            :char
    :pred            string?
    :type-properties {:error/message "should be a string"}}))


(def jsonb
  (m/-simple-schema
   {:type            :jsonb
    :pred            #(or (map? %) (vector? %))
    :type-properties {:error/message "should be a map or vector"}}))


(def timestamp
  (m/-simple-schema
   {:type            :timestamp
    :pred            #(instance? LocalDateTime %)
    :type-properties {:error/message "should be an instance of LocalDateTime"}}))


(def timestamp-tz
  (m/-simple-schema
   {:type            :timestamp-tz
    :pred            #(instance? OffsetDateTime %)
    :type-properties {:error/message "should be an instance of OffsetDateTime"}}))


(def date
  (m/-simple-schema
   {:type            :date
    :pred            #(instance? LocalDate %)
    :type-properties {:error/message "should be an instance of LocalDate"}}))


(def time
  (m/-simple-schema
   {:type            :time
    :pred            #(instance? LocalTime %)
    :type-properties {:error/message "should be an instance of LocalTime"}}))


(def time-tz
  (m/-simple-schema
   {:type            :time-tz
    :pred            #(instance? OffsetTime %)
    :type-properties {:error/message "should be an instance of OffsetTime"}}))


(def interval
  (m/-simple-schema
   {:type            :interval
    :pred            #(instance? Duration %)
    :type-properties {:error/message "should be an instance of Duration"}}))


(def array
  (m/-simple-schema
   {:type            :array
    :pred            sequential?
    :type-properties {:error/message "should be sequential"}}))

