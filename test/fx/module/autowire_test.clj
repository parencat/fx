(ns fx.module.autowire-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [fx.module.autowire :as sut]
   [malli.core :as m]
   [malli.instrument :as mi]))


(duct/load-hierarchy)

(mi/instrument!)


(deftest find-project-namespaces-test
  (let [result-spec [:sequential {:min 1} :symbol]]
    (testing "output result"
      (is (m/validate result-spec (sut/find-project-namespaces 'fx.module))
          "result should be a sequence of symbols"))

    (testing "input parameter"
      (is (m/validate result-spec (sut/find-project-namespaces "fx.module"))
          "input can be a string as well")

      (testing "keywords not supported as input"
        (is (thrown? Exception
                     (sut/find-project-namespaces :fx.module)))

        (testing "wouldn't throw w/o malli instrumentation, but return nothing"
          (mi/unstrument!)
          (is (empty? (sut/find-project-namespaces :fx.module)))
          (mi/instrument!)))

      (is (m/validate result-spec (sut/find-project-namespaces))
          "should work w/o any parameters")

      (is (not= (sut/find-project-namespaces nil)
                (sut/find-project-namespaces))
          "passing nil isn't the same as calling w/o argument"))))


(def valid-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.module.something}})


(deftest autowire-config-prep
  (let [config (duct/prep-config valid-config)
        system (ig/init config)]

    (testing "basic component"
      (is (some? (:fx.module.something/health-check system)))
      (is (= {:status :ok}
             ((:fx.module.something/health-check system) {} {}))))

    (testing "parent - child component"
      (is (some? (get system :fx.module.something/status)))
      (is (= {:status     :ok
              :connection {:connected :ok}}
             ((get system :fx.module.something/status)))))

    (ig/halt! system)))


(deftest autowire-di-test
  (let [config (duct/prep-config valid-config)
        system (ig/init config)]

    (testing "dependency injection configured properly"
      (let [status-handler-conf (get config :fx.module.something/status)
            db-connection-conf  (:fx.module.something/db-connection config)]
        (is (some? (:db-connection status-handler-conf)))
        (is (ig/ref? (:db-connection status-handler-conf)))

        (is (some? db-connection-conf))

        (testing "components return correct results"
          (let [status-handler (get system :fx.module.something/status)]
            (is (= {:status     :ok
                    :connection {:connected :ok}}
                   (status-handler)))))))

    (ig/halt! system)))
