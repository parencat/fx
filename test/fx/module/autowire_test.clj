(ns fx.module.autowire-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [fx.module.autowire :as sut]
   [malli.core :as m]
   [malli.instrument :as mi]
   [fx.module.stub-functions :as sf]))


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


(deftest collect-autowired-test
  (testing "components w/o metadata should be skipped"
    (is (empty?
         (sut/collect-autowired "some-ns" {} 'my-component {:some "value"}))))

  (testing "component should be added to the config map"
    (let [component-val (with-meta {:some "value"} {sut/AUTOWIRED-KEY true})
          result        (sut/collect-autowired "some-ns" {} 'my-component component-val)]
      (is (contains? result :some-ns/my-component))
      (is (identical? (get result :some-ns/my-component)
                      component-val)))))


(deftest find-components-test
  (testing "returns all autowired items as map of names to vars"
    (let [result (sut/find-components '(fx.module.stub-functions))]
      (is (contains? result :fx.module.stub-functions/constant-value))
      (is (= (get result :fx.module.stub-functions/constant-value)
             (var fx.module.stub-functions/constant-value)))))

  (testing "for ns without autowired members should return an empty map"
    (is (empty? (sut/find-components '(fx.module.autowire-test))))))


(defn simple-func [num]
  (+ num num))


(def simple-val
  [1 2 3])


(defn multi-arity-func
  ([a] (inc a))
  ([a b] (+ a b)))


(deftest get-comp-deps-test
  (testing "returns empty collection if no metadata set on function"
    (is (empty? (sut/get-comp-deps (meta #'simple-func))))
    (is (empty? (sut/get-comp-deps (meta #'simple-val))))
    (is (empty? (sut/get-comp-deps (meta #'sf/health-check)))))

  (testing "returns all autowired items as map of names to vars"
    (let [result (sut/get-comp-deps (meta #'sf/status))]
      (is (vector? result))
      (is (= 1 (count result)))
      (is (= :fx.module.stub-functions/db-connection (first result)))))

  (testing "multi-arity functions not supported"
    (is (thrown? Exception
                 (sut/get-comp-deps (meta #'multi-arity-func))))))


(deftest prep-component-test
  (let [result (sut/prep-component {} :fx.module.stub-functions/status #'sf/status)]
    (is (contains? result :fx.module.stub-functions/status))
    (is (ig/ref? (get-in result [:fx.module.stub-functions/status :db-connection])))

    (testing "integrant methods should be in place"
      (testing "init key"
        (let [init-method   (get-method ig/init-key :fx.module.stub-functions/status)
              method-result (init-method nil {:db-connection (fn [] :connected)})]
          (is (= {:connection :connected
                  :status     :ok}
                 (method-result)))))

      (testing "halt key"
        (let [_             (sut/prep-component {} :fx.module.stub-functions/close-connection #'sf/close-connection)
              halt-method   (get-method ig/halt-key! :fx.module.stub-functions/db-connection)
              method-result (halt-method nil nil)]
          (is (= :closed method-result)))))))



;; =============================================================================
;; System tests
;; =============================================================================

(def app-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.module.stub-functions}})


(deftest autowire-config-prep
  (let [config (duct/prep-config app-config)
        system (ig/init config)]

    (testing "basic component"
      (let [health-check (:fx.module.stub-functions/health-check system)]
        (is (some? health-check))
        (is (fn? health-check))
        (is (= {:status :ok} (health-check {} {})))))

    (testing "parent - child component"
      (let [status (get system :fx.module.stub-functions/status)]
        (is (some? status))
        (is (fn? status))
        (is (= {:status     :ok
                :connection {:connected :ok}}
               (status)))))

    (ig/halt! system)))


(deftest autowire-di-test
  (let [config (duct/prep-config app-config)
        system (ig/init config)]

    (testing "dependency injection configured properly"
      (let [status-handler-conf (get config :fx.module.stub-functions/status)
            db-connection-conf  (get config :fx.module.stub-functions/db-connection)]
        (is (contains? status-handler-conf :db-connection))
        (is (ig/ref? (get status-handler-conf :db-connection)))
        (is (some? db-connection-conf))

        (testing "components return correct results"
          (let [status-handler (get system :fx.module.stub-functions/status)]
            (is (= {:status     :ok
                    :connection {:connected :ok}}
                   (status-handler)))))))

    (ig/halt! system)))


(deftest autowire-parent-components
  (let [config (duct/prep-config app-config)
        system (ig/init config)]

    (testing "when using parent components keys represented in config as composite"
      (let [composite-key [:fx.module.stub-functions/test :fx.module.stub-functions/child-test-component]]
        (is (contains? config composite-key))
        (is (= :test (get-in config [composite-key :component])))))

    (ig/halt! system)))
