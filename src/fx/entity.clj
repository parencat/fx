(ns fx.entity
  (:require
   [integrant.core :as ig]))

(defprotocol PRepository
  (create! [_])
  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))


(defrecord Entity []
  PRepository
  (create! [_])
  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))


(defmethod ig/init-key :fx/entity [_ config]
  ;; create ddl
  ;; return entity record
  (map->Entity {}))
