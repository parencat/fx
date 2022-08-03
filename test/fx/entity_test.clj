(ns fx.entity-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [next.jdbc :as jdbc]
   [malli.instrument :as mi]
   [fx.entity :as fx.entity])
  (:import
   [java.sql Connection]))


(duct/load-hierarchy)
(mi/instrument!)


(def ^{:fx/autowire :fx/entity} dumb-entity
  [:spec {:table "dumb-test"}
   [:id {:primary-key? true} [:string {:max 250}]]])


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:last-name {:optional? true} string?]
   [:client {:many-to-one? true} :fx.entity-test/client]
   [:role {:many-to-one? true} :fx.entity-test/role]])


(def ^{:fx/autowire :fx/entity} client
  [:spec {:table "client"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:one-to-many? true} :fx.entity-test/user]])


(def ^{:fx/autowire :fx/entity} role
  [:spec {:table "role"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:one-to-many? true} :fx.entity-test/user]])


(def simple-config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.entity-test}
   :fx.module/database                {:migrate {:strategy :update-drop}}
   :fx.containers.postgres/connection {}})


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
        (is (satisfies? fx.entity/PEntity role))
        (is (satisfies? fx.entity/PEntity client))
        (is (satisfies? fx.entity/PEntity user))

        (testing "should create records in database"
          (let [_role-res   (fx.entity/create! role {:id role-id :name "test-role"})
                _client-res (fx.entity/create! client {:id client-id :name "test-client"})
                user-res    (fx.entity/create! user {:id     user-id :name "test-user" :last-name "test-last"
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
