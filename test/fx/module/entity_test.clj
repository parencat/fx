(ns fx.module.entity-test
  (:require [clojure.test :refer :all]
            [duct.core :as duct]
            [integrant.core :as ig]))


(duct/load-hierarchy)


(def simple-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.module}
   :fx.module/entity   (ig/ref :fx.module/autowire)})


(def ^{:fx/autowire :fx/entity} test-entity
  [:table "definition"])


(deftest entity-module-test
  (let [config (duct/prep-config simple-config)]

    (testing "should create an entity key"
      (is (some? (get config [:fx/entity :fx.module.entity-test/test-entity])))

      (is (= #{:table :database}
             (-> (get config [:fx/entity :fx.module.entity-test/test-entity])
                 keys
                 set))))))
