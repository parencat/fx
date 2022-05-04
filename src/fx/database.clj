(ns fx.database
  (:require
   [integrant.core :as ig]
   [next.jdbc :as jdbc])
  (:import
   [java.sql Connection]))


(defmethod ig/init-key :fx/database [_ {:keys [url]}]
  (jdbc/get-connection {:jdbcUrl url}))


(defmethod ig/halt-key! :fx/database [^Connection connection]
  (.close connection))
