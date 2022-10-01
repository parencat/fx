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
   [:addresses {:rel-type :one-to-many} :fx.entity-test/address]
   [:pol {:rel-type :one-to-one} :fx.entity-test/address]
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


(def ^{:fx/autowire :fx/entity} address
  [:spec
   [:id {:identity true} uuid?]
   [:street :string]
   [:post-code :string]])


(def config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.entity-test}})


(deftest entity-module-test
  (let [config (duct/prep-config config)]

    (testing "should create an entity key in the prepared config"
      (is (some? (get config [:fx/entity :fx.entity-test/dumb-entity]))))

    (testing "should create Entity records for declared entities in the running system"
      (let [system (ig/init config)
            role   (get system [:fx/entity :fx.entity-test/role])
            client (get system [:fx/entity :fx.entity-test/client])
            user   (get system [:fx/entity :fx.entity-test/user])]
        (is (instance? Entity role))
        (is (instance? Entity client))
        (is (instance? Entity user))

        (is (= (:type user) :fx.entity-test/user))

        (ig/halt! system)))))


(deftest entity-validation-test
  (let [config (duct/prep-config config)
        system (ig/init config)
        user   (val (ig/find-derived-1 system ::user))
        role   (val (ig/find-derived-1 system ::role))]

    (testing "validation should figure out nested records and validate only identity field"
      (is (fx.entity/valid-entity?
           user
           {:id        (random-uuid)
            :name      "Name"
            :last-name "Lastname"
            :pol       {:id     (random-uuid)
                        :street "Street"}
            :addresses [{:id        (random-uuid)
                         ;; :post-code declared as string but not encountered in validation
                         :post-code 123456}]
            :client    (random-uuid)
            :role      {:id (random-uuid)}})))

    (testing "sequential relation should expect a sequence of nested records"
      (is (fx.entity/valid-entity? role {:id   (random-uuid)
                                         :name "Role"
                                         :user [{:id (random-uuid)}]}))

      (is (fx.entity/valid-entity? role {:id   (random-uuid)
                                         :name "Role"
                                         :user [(random-uuid)]}))

      (testing "should thrown an error otherwise"
        (is (thrown? Exception
                     (fx.entity/valid-entity? role {:id   (random-uuid)
                                                    :name "Role"
                                                    ;; user should be sequence
                                                    :user {:id (random-uuid)}})))

        (is (thrown? Exception
                     (fx.entity/valid-entity? role {:id   (random-uuid)
                                                    :name "Role"
                                                    ;; user should be sequence
                                                    :user (random-uuid)})))))

    (ig/halt! system)))
