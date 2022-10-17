(ns fx.constraints-test
  (:require
   [clojure.test :refer :all]
   [fx.system :refer [with-system]]
   [fx.migrate]
   [fx.repo]
   [duct.core :as duct]
   [malli.instrument :as mi]
   [integrant.core :as ig]
   [fx.entity :as entity]))


(duct/load-hierarchy)
(mi/instrument!)


(def config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.constraints-test}
   :fx.module/repo                    {:migrate {:strategy :update-drop}}
   :fx.containers.postgres/connection {}})


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:identity true} :uuid]
   [:first-name {:default "Uncle Bob"} :string]])


(def mod-user-spec
  [:spec {:table "user"}
   [:id {:identity true} :uuid]
   [:first-name {:default "Jack Sparrow"} :string]])


(deftest default-value-test
  (with-system [system config]
    (let [ds   (val (ig/find-derived-1 system :fx.database/connection))
          user (val (ig/find-derived-1 system :fx.constraints-test/user))
          leo  (random-uuid)
          bob  (random-uuid)]
      (is (fx.migrate/table-exist? ds "user"))

      (testing "saving with default value"
        (fx.repo/save! user {:id         leo
                             :first-name "Leo"})

        (let [{:keys [first-name]} (fx.repo/find! user {:id leo})]
          (is (= "Leo" first-name)))

        (fx.repo/save! user {:id bob})

        (let [{:keys [first-name]} (fx.repo/find! user {:id bob})]
          (is (= "Uncle Bob" first-name))))

      (testing "altering default value"
        (let [user-spec   (-> mod-user-spec entity/prepare-spec :spec)
              user-entity (entity/create-entity :fx.constraints-test/user user-spec)
              jack        (random-uuid)]

          (fx.migrate/apply-migrations! {:database ds
                                         :entities #{user-entity}})

          (fx.repo/save! user {:id jack})

          (let [{:keys [first-name]} (fx.repo/find! user {:id jack})]
            (is (= "Jack Sparrow" first-name))))))))


