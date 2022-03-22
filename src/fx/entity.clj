(ns fx.entity
  (:require
   [integrant.core :as ig]))

(defprotocol PRepository
  (create! [])
  (update! [])
  (find! [])
  (find-all! [])
  (delete! []))


(defrecord Entity []
  PRepository
  (create! [])
  (update! [])
  (find! [])
  (find-all! [])
  (delete! []))


(defmethod ig/init-key :fx/entity [_ config]
  ;; create ddl
  ;; return entity record
  (map->Entity {}))
