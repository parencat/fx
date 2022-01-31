(ns fx-demo.autowire-test
  (:require [clojure.test :refer :all]
            [duct.core :as duct]
            [integrant.core :as integrant]
            [modules.autowire :as autowire]))


(duct/load-hierarchy)


(def valid-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:project-ns 'fx-demo}})


(deftest autowire-config-prep
  (let [config (duct/prep-config valid-config)
        system (integrant/init config)]

    (testing "basic component"
      (is (some? (:fx-demo.something/health-check system)))
      (is (= {:status :ok}
             ((:fx-demo.something/health-check system) {} {}))))

    (testing "parent - child component"
      (is (some? (get system [:http-server/handler :fx-demo.something/status])))
      (is (= {:status :ok}
             ((get system [:http-server/handler :fx-demo.something/status]) {} {}))))

    (integrant/halt! system)))
