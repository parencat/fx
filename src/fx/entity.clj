(ns fx.entity
  (:require
   [integrant.core :as ig]
   [fx.repository :as repo]
   [malli.core :as m]
   [next.jdbc :as jdbc]
   [honey.sql :as sql]))


(defrecord Entity [database create]
  repo/PRepository
  (create! [_ params]
    (let [query (-> (:statement create)
                    (assoc :columns (:columns create))
                    (assoc :values [((apply juxt (:columns create)) params)])
                    (sql/format))]
      (println query)
      #_(jdbc/execute-one! database query)))

  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))


(defmethod ig/prep-key :fx/entity [_ table]
  ;; inject database connection reference
  {:table    table
   :database (ig/ref :fx/database)})


;; create ddl
;; return entity record
(def Table
  [:schema
   {:registry {"table" [:catn
                        [:type [:enum :table]]
                        [:props [:? [:map-of keyword? any?]]]
                        [:fields [:* [:schema [:ref "field"]]]]]
               "field" [:catn
                        [:name keyword?]
                        [:props [:? [:map-of keyword? any?]]]
                        [:type [:or fn? keyword?]]]}}
   "table"])


(def parse-table
  (m/parser Table))


(defmethod ig/init-key :fx/entity [_ config]
  (let [{:keys [table database]} config
        parsed-table     (parse-table table)
        create-statement {:insert-into (get-in parsed-table [:props :name])}]
    (map->Entity {:database database
                  :create   {:statement create-statement
                             :columns   (mapv :name (:fields parsed-table))}})))


(comment
 (def Table
   [:schema
    {:registry {"table" [:catn
                         [:type [:enum :table]]
                         [:props [:? [:map-of keyword? any?]]]
                         [:fields [:* [:schema [:ref "field"]]]]]
                "field" [:catn
                         [:name keyword?]
                         [:props [:? [:map-of keyword? any?]]]
                         [:type [:or fn? keyword?]]]}}
    "table"])

 (def parse-table
   (m/parser Table))

 (def user-tbl
   [:table {:name "user"}
    [:id {:primary-key? true} uuid?]
    [:name {:type "text"} string?]
    [:last-name {:optional? true} string?]
    [:client {:has-one? true} :ddd.service/client]
    [:role {:has-many? true} :ddd.service/role]])

 (def user-table-parsed
   (parse-table user-tbl))

 (sql/format {:insert-into (get-in user-table-parsed [:props :name])
              :columns     [:id :name]})

 (let [create-statement {:insert-into (get-in user-table-parsed [:props :name])}
       ^Entity user (map->Entity {:database "database"
                                  :create   {:statement create-statement
                                             :columns   (mapv :name (:fields user-table-parsed))}})]
   (.create! user {:id "qwe" :name "test"}))

 ((apply juxt [:id :name]) {:id "qwe" :name "test"}))



