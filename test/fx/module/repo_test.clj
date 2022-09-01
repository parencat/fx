(ns fx.module.repo-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [malli.instrument :as mi]
   [fx.repo]))


(duct/load-hierarchy)
(mi/instrument!)


(def ^{:fx/autowire :fx/entity} person
  [:spec {:table "person"}
   [:id {:primary-key? true} :uuid]
   [:name :string]
   [:column {:wrap? true} :string]])


(def config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.module.repo-test}
   :fx.module/repo                    {:migrate {:strategy :update-drop}}
   :fx.containers.postgres/connection {}})


(deftest repo-module-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (testing "repo module initialized all sub components"
      (is (not-empty (ig/find-derived-1 system :fx.repo/migrate)))
      (is (not-empty (ig/find-derived-1 system :fx.repo/adapter))))

    (testing "entity extended by repo module"
      (let [person    (val (ig/find-derived-1 system ::person))
            person-id (random-uuid)]
        (fx.repo/save! person {:id     person-id
                               :column "base"
                               :name   "Mighty Repo"})

        (is (= (fx.repo/find! person {:id person-id})
               {:id     person-id
                :column "base"
                :name   "Mighty Repo"}))))

    (ig/halt! system)))

