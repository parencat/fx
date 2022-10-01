(ns fx.entity-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [malli.instrument :as mi]
   [fx.entity])
  (:import
   [fx.entity Entity]))


(duct/load-hierarchy)
(mi/instrument!)


(def ^{:fx/autowire :fx/entity} dumb-entity
  [:spec {:table "dumb-test"}
   [:id {:identity true} [:string {:max 250}]]
   [:user {:rel-type :many-to-one} :fx.entity-test/user]])


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:identity true} uuid?]
   [:name [:string {:max 250}]]
   [:last-name {:optional true} string?]
   [:client {:rel-type :many-to-one} :fx.entity-test/client]
   [:role {:rel-type :many-to-one} :fx.entity-test/role]])


(def ^{:fx/autowire :fx/entity} client
  [:spec {:table "client"}
   [:id {:identity true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:rel-type :one-to-many} :fx.entity-test/user]])


(def ^{:fx/autowire :fx/entity} role
  [:spec {:table "role"}
   [:id {:identity true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:rel-type :one-to-many} :fx.entity-test/user]])


(def simple-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.entity-test}})


(deftest entity-module-test
  (let [config (duct/prep-config simple-config)]

    (testing "should create an entity key in the prepared config"
      (is (some? (get config [:fx/entity :fx.entity-test/dumb-entity]))))

    (testing "should create Entity records for declared entities in the running system"
      (let [system (ig/init config)
            Role   (get system [:fx/entity :fx.entity-test/role])
            Client (get system [:fx/entity :fx.entity-test/client])
            User   (get system [:fx/entity :fx.entity-test/user])]
        (is (instance? Entity Role))
        (is (instance? Entity Client))
        (is (instance? Entity User))

        (is (= (:type User) :fx.entity-test/user))

        (ig/halt! system)))))
