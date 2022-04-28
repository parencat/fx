(ns fx.entity
  (:require
   [integrant.core :as ig]
   [fx.repository :as repo]
   [malli.core :as m]
   [next.jdbc :as jdbc]
   [honey.sql :as sql]))


(defrecord Entity [database table]
  repo/PRepository
  (create! [_ entity]
    (let [table-name  (get-in table [:props :name])
          columns     (mapv :name (:fields table))
          get-values (apply juxt columns)
          query       (-> {:insert-into table-name}
                          (assoc :columns columns)
                          (assoc :values [(get-values entity)])
                          (sql/format))]
      (println query)
      ;; TODO validate data before execution
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
        parsed-table (parse-table table)]
    ;; TODO add parsing validation, throw error on invalid table
    (map->Entity {:database database
                  :table    parsed-table})))


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
       ^Entity user     (map->Entity {:database "database"
                                      :create   {:statement create-statement
                                                 :columns   (mapv :name (:fields user-table-parsed))}})]
   (repo/create! user {:id "qwe" :name "test"}))

 ((apply juxt [:id :name]) {:id "qwe" :name "test"}))



