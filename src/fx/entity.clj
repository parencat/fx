(ns fx.entity
  (:require
   [integrant.core :as ig]
   [fx.repository :as repo]))


(defrecord Entity []
  repo/PRepository
  (create! [_])
  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))


(defmethod ig/prep-key :fx/entity [_ table]
  ;; inject database connection reference
  {:table    table
   :database (ig/ref :fx/database)})


(defmethod ig/init-key :fx/entity [_ config]
  ;; create ddl
  ;; return entity record
  (map->Entity {}))
