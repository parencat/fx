(ns fx.migrate-test
  (:require
   [clojure.test :refer :all]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [fx.containers.postgres :as pg])
  (:import [java.sql DatabaseMetaData]))


(def pgc
  (pg/pg-container))


(def con
  (jdbc/get-connection
   {:jdbcUrl  (.getJdbcUrl (:container pgc))
    :user     (.getUsername (:container pgc))
    :password (.getPassword (:container pgc))}))


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


(-> ^DatabaseMetaData (.getMetaData con)
    (.getTables nil nil nil (into-array ["TABLE"]))
    (rs/datafiable-result-set con {:builder-fn rs/as-unqualified-kebab-maps})
    ((partial map #(select-keys % [:table-schem :table-name]))))


(-> ^DatabaseMetaData (.getMetaData con)
    (.getColumns nil "public" nil nil)
    (rs/datafiable-result-set con {:builder-fn rs/as-unqualified-kebab-maps})
    ((partial map #(select-keys % [:column-name :table-name :ordinal-position :type-name])))
    ((partial group-by :table-name)))


;; from DB
{"client" [{:column-name "id", :table-name "client", :ordinal-position 1, :type-name "uuid"}
           {:column-name "name", :table-name "client", :ordinal-position 2, :type-name "varchar(250)"}],
 "role"   [{:column-name "id", :table-name "role", :ordinal-position 1, :type-name "uuid"}
           {:column-name "name", :table-name "role", :ordinal-position 2, :type-name "varchar"}],
 "user"   [{:column-name "id", :table-name "user", :ordinal-position 1, :type-name "uuid"}
           {:column-name "name", :table-name "user", :ordinal-position 2, :type-name "varchar"}
           {:column-name "last_name", :table-name "user", :ordinal-position 3, :type-name "varchar"}
           {:column-name "client", :table-name "user", :ordinal-position 4, :type-name "uuid"}
           {:column-name "role", :table-name "user", :ordinal-position 5, :type-name "uuid"}]}


(require '[clojure.data :as data])


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

{:alter-table :fruit
 :add-column  [:id :int [:not nil]]}

{:alter-table :fruit
 :drop-column :ident}

{:alter-table   :fruit
 :modify-column [:id :int :unsigned nil]}
