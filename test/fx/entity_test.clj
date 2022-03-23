(ns fx.entity-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]))


(duct/load-hierarchy)


(def simple-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.entity-test}
   :fx.module/database {}})


(def ^{:fx/autowire :fx/entity} test-entity
  [:table "definition"])


(deftest entity-module-test
  (let [config (duct/prep-config simple-config)]

    (testing "should create an entity key"
      (is (some? (get config [:fx/entity :fx.entity-test/test-entity])))

      (is (= #{:table :database}
             (-> (get config [:fx/entity :fx.entity-test/test-entity])
                 keys
                 set))))))
