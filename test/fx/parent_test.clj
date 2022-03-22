(ns fx.parent-test
  (:require [clojure.test :refer :all]
            [duct.core :as duct]
            [integrant.core :as ig]))


(duct/load-hierarchy)


(def simple-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx}})


(defmethod ig/init-key :fx/parent [_ config]
  (->> config
       (map-indexed (fn [idx elem] [idx elem]))
       (into {})))


(def ^{:fx/autowire :fx/parent} my-parent
  [:I'm "a" 'parent "config"])


(deftest composite-key-test
  (let [config (duct/prep-config simple-config)]

    (testing "should create a composite key"
      (is (some? (get config [:fx/parent :fx.parent-test/my-parent])))

      (is (= [:I'm "a" 'parent "config"]
             (get config [:fx/parent :fx.parent-test/my-parent]))))))


(deftest composite-key-system-test
  (let [config (duct/prep-config simple-config)
        system (ig/init config)]

    (testing "composite key should process the config value"
      (is (= {0 :I'm
              1 "a"
              2 'parent
              3 "config"}
             (get system [:fx/parent :fx.parent-test/my-parent]))))

    (ig/halt! system)))
