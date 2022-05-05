(ns fx.entity-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [fx.repository :as repo]
   [next.jdbc :as jdbc])
  (:import
   [java.sql Connection]))


(duct/load-hierarchy)


(def ^{:fx/autowire :fx/entity} dumb-entity
  [:table {:name "dumb-test"}
   [:id {:primary-key? true} [:string {:max 250}]]])


(def ^{:fx/autowire :fx/entity} user
  [:table {:name "user"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:last-name {:optional? true} string?]
   [:client {:many-to-one? true} :fx.entity-test/client]
   [:role {:many-to-one? true} :fx.entity-test/role]])


(def ^{:fx/autowire :fx/entity} client
  [:table {:name "client"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:one-to-many? true} :fx.entity-test/user]])


(def ^{:fx/autowire :fx/entity} role
  [:table {:name "role"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:one-to-many? true} :fx.entity-test/user]])


(def simple-config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.entity-test}
   :fx.containers.postgres/connection {}})


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


(defn setup-tables [^Connection connection]
  (jdbc/execute! connection [tables-query]))

(defn query-user [^Connection connection id]
  (jdbc/execute-one! connection ["select * from \"user\" where id = ?" id]))

(defn query-role [^Connection connection id]
  (jdbc/execute-one! connection ["select * from role where id = ?" id]))

(defn query-client [^Connection connection id]
  (jdbc/execute-one! connection ["select * from client where id = ?" id]))


(deftest entity-module-test
  (let [config (duct/prep-config simple-config)]

    (testing "should create an entity key in the prepared config"
      (is (some? (get config [:fx/entity :fx.entity-test/dumb-entity]))))


    (testing "should create clojure records for entities in the running system"
      (let [system     (ig/init config)
            role       (get system [:fx/entity :fx.entity-test/role])
            client     (get system [:fx/entity :fx.entity-test/client])
            user       (get system [:fx/entity :fx.entity-test/user])
            role-id    (random-uuid)
            client-id  (random-uuid)
            user-id    (random-uuid)
            connection (get system :fx.database/connection)]
        (is (satisfies? repo/PRepository role))
        (is (satisfies? repo/PRepository client))
        (is (satisfies? repo/PRepository user))

        (testing "should create records in database"
          (setup-tables connection)

          (let [_role-res   (repo/create! role {:id role-id :name "test-role"})
                _client-res (repo/create! client {:id client-id :name "test-client"})
                user-res    (repo/create! user {:id     user-id :name "test-user" :last-name "test-last"
                                                :client client-id :role role-id})

                new-role    (query-role connection role-id)
                new-client  (query-client connection client-id)
                new-user    (query-user connection user-id)]

            (is (= (:id user-res) user-id))
            (is (= (:last-name user-res) "test-last"))

            (is (= (:role/name new-role) "test-role"))
            (is (= (:client/name new-client) "test-client"))
            (is (= (:user/name new-user) "test-user"))))

        (ig/halt! system)))))
