(ns fx.module.autowire-test
  (:require [clojure.test :refer :all]
            [duct.core :as duct]
            [integrant.core :as integrant]
            [fx.module.autowire :as autowire]))


(duct/load-hierarchy)


(def valid-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.module.something}})


(deftest autowire-config-prep
  (let [config (duct/prep-config valid-config)
        system (integrant/init config)]

    (testing "basic component"
      (is (some? (:fx.module.something/health-check system)))
      (is (= {:status :ok}
             ((:fx.module.something/health-check system) {} {}))))

    (testing "parent - child component"
      (is (some? (get system :fx.module.something/status)))
      (is (= {:status     :ok
              :connection {:connected :ok}}
             ((get system :fx.module.something/status)))))

    (integrant/halt! system)))


(deftest autowire-di-test
  (let [config (duct/prep-config valid-config)
        system (integrant/init config)]

    (testing "dependency injection configured properly"
      (let [status-handler-conf (get config :fx.module.something/status)
            db-connection-conf  (:fx.module.something/db-connection config)]
        (is (some? (:db-connection status-handler-conf)))
        (is (integrant/ref? (:db-connection status-handler-conf)))

        (is (some? db-connection-conf))

        (testing "components return correct results"
          (let [status-handler (get system :fx.module.something/status)]
            (is (= {:status     :ok
                    :connection {:connected :ok}}
                   (status-handler)))))))

    (integrant/halt! system)))
