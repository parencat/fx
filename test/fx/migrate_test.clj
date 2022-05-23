(ns fx.migrate-test
  (:require
   [clojure.test :refer :all]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [fx.containers.postgres :as pg]
   [honey.sql :as sql])
  (:import [java.sql DatabaseMetaData Types]))

;;
;;(def pgc
;;  (pg/pg-container))
;;
;;
;;(def con
;;  (jdbc/get-connection
;;   {:jdbcUrl  (.getJdbcUrl (:container pgc))
;;    :user     (.getUsername (:container pgc))
;;    :password (.getPassword (:container pgc))}))

(require '[fx.migrate :as migrate])
(require '[fx.entity :as entity])

(def con (:fx.database/connection 'system))

(def user
  (entity/create-entity :user {:table    [:table {:name "user"}
                                          [:id {:primary-key? true} uuid?]
                                          [:name [:string {:max 250}]]
                                          [:last-name {:optional? true} string?]
                                          [:client {:many-to-one? true} :fx.entity-test/client]
                                          [:role {:many-to-one? true} :fx.entity-test/role]]
                               :database con}))


(def entities
  (-> (select-keys {} [[:fx/entity :fx.entity-test/role]
                       [:fx/entity :fx.entity-test/user]
                       [:fx/entity :fx.entity-test/client]])
      vals
      set))

(migrate/extract-db-columns con entities)
(migrate/get-table-columns con "user")
(migrate/db->simple-schema (migrate/extract-db-columns con entities))
(migrate/entities->simple-schema @entity/entities-lookup entities)

(data/diff (migrate/db->simple-schema (migrate/extract-db-columns con entities))
           (migrate/entities->simple-schema @entity/entities-lookup entities))

(data/diff {"role" {"id" {:type "uuid", :nullable false, :primary-key true},
                    "name" {:type "varchar", :nullable false},
                    "test-name" {:type "varchar", :nullable true}},
            "user" {"id" {:type "uuid", :nullable false, :primary-key true},
                    "name" {:type "varchar", :nullable false},
                    "last-name" {:type "varchar", :nullable true},
                    "client" {:type "uuid", :nullable false, :foreign-key true, :references "client"},
                    "role" {:type "uuid", :nullable false, :foreign-key true, :references "role"}}}

           {"role" {"id" {:type "uuid", :nullable false, :primary-key true},
                    "name" {:type "varchar", :nullable false},
                    "test-name" {:type "varchar", :nullable true}},
            "client" {"id" {:type "uuid", :nullable false, :primary-key true}, "name" {:type "varchar", :nullable false}},
            "user" {"id" {:type "uuid", :nullable false, :primary-key true},
                    "name" {:type "varchar", :nullable false},
                    "last-name" {:type "varchar", :nullable true},
                    "client" {:type "uuid", :nullable false, :foreign-key true, :references "client"},
                    "role" {:type "uuid", :nullable false, :foreign-key true, :references "role"}}})


(def tables-query
  "create table client (
    id   uuid    not null primary key,
    name varchar not null unique
   );
   create table role (
    id   uuid    not null primary key,
    name varchar not null unique
   );
   create table \"user\" (
    id        uuid    not null primary key,
    name      varchar not null,
    last_name varchar,
    client    uuid    not null references client on delete cascade,
    role      uuid    not null references role on delete cascade
   );")


(jdbc/execute! con [tables-query])
(jdbc/execute! con ["alter table role add column test_name varchar (250);"])


(-> ^DatabaseMetaData (.getMetaData con)
    (.getTables nil nil nil (into-array ["TABLE"]))
    (rs/datafiable-result-set con {:builder-fn rs/as-unqualified-kebab-maps})
    ((partial map #(select-keys % [:table-schem :table-name]))))


(-> ^DatabaseMetaData (.getMetaData con)
    (.getColumns nil "public" "user" nil)
    (rs/datafiable-result-set con {:builder-fn rs/as-unqualified-kebab-maps}))

(-> ^DatabaseMetaData (.getMetaData con)
    (.getPrimaryKeys nil nil "user")
    (rs/datafiable-result-set con {:builder-fn rs/as-unqualified-kebab-maps}))

(-> ^DatabaseMetaData (.getMetaData con)
    (.getImportedKeys nil nil "user")
    (rs/datafiable-result-set con {:builder-fn rs/as-unqualified-kebab-maps}))



;; from DB
(def sample
  {"client" {"id"   {:type "uuid"}
             "name" {:type "varchar(250)"}}

   "role"   {"id",  {:type "uuid"}
             "name" {:type "varchar"}}

   "user"   {"id"        {:type "uuid"}
             "name"      {:type "varchar"}
             "last_name" {:type "varchar"}
             "client"    {:type "uuid"}
             "role"      {:type "uuid"}}})


(require '[clojure.data :as data])

(data/diff sample
           {"client" {"id"   {:type "uuid"}
                      "name" {:type "varchar(30)"}}

            "user"   {"id"     {:type "uuid"}
                      "name"   {:type "varchar"}
                      "client" {:type "uuid"}
                      "role"   {:type "uuid"}}})


({"user"   {"last_name" {:type "varchar"}},
  "client" {"name" {:type "varchar(250)"}},
  "role"   {"id" {:type "uuid"}, "name" {:type "varchar"}}}

 {"client" {"name" {:type "varchar(30)"}}}

 {"user"   {"role" {:type "uuid"}, "client" {:type "uuid"}, "name" {:type "varchar"}, "id" {:type "uuid"}},
  "client" {"id" {:type "uuid"}}})


;; schemas lookup
{:fx.entity-test/client {:type :map,
                         :keys {:id   {:order      0,
                                       :value      {:type uuid?},
                                       :properties {:primary-key? true}},
                                :name {:order      1,
                                       :value      {:type :string, :properties {:max 250}},
                                       :properties nil},
                                :user {:order      2,
                                       :value      {:type :+, :children [{:type :fx.entity-test/user-ref}]},
                                       :properties {:one-to-many? true, :optional true}}}},
 :fx.entity-test/role   {:type :map,
                         :keys {:id   {:order      0,
                                       :value      {:type uuid?},
                                       :properties {:primary-key? true}},
                                :name {:order      1,
                                       :value      {:type :string, :properties {:max 250}},
                                       :properties nil},
                                :user {:order      2,
                                       :value      {:type :+, :children [{:type :fx.entity-test/user-ref}]},
                                       :properties {:one-to-many? true, :optional true}}}},
 :fx.entity-test/user   {:type :map,
                         :keys {:id        {:order      0,
                                            :value      {:type uuid?},
                                            :properties {:primary-key? true}},
                                :name      {:order      1,
                                            :value      {:type :string, :properties {:max 250}},
                                            :properties nil},
                                :last-name {:order      2,
                                            :value      {:type string?},
                                            :properties {:optional true}},
                                :client    {:order      3,
                                            :value      {:type :fx.entity-test/client-ref},
                                            :properties {:many-to-one? true}},
                                :role      {:order      4,
                                            :value      {:type :fx.entity-test/role-ref},
                                            :properties {:many-to-one? true}}}},}

;; honeysql format
{:drop-table [:foo :bar]}

{:create-table "user"
 :with-columns
 [[:id :uuid [:not nil] [:primary-key]]
  [:name :varchar [:not nil]]
  [:last-name :varchar]
  [:client :uuid [:not nil] [:references :client :id]]
  [:role :uuid [:not nil] [:references :role]]]}

(sql/format {:create-table "user"
             :with-columns
             [[:id :uuid [:not nil] [:primary-key]]
              [:name :varchar [:not nil]]
              [:last-name :varchar]
              [:client :uuid [:not nil] [:references :client]]
              [:role :uuid [:not nil] [:references :role]]]})

{:alter-table :fruit
 :add-column  [:id :int [:not nil]]}

{:alter-table :fruit
 :drop-column :ident}

{:alter-table   :fruit
 :modify-column [:id :int :unsigned nil]}
