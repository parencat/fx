(ns todo.core-test
  (:require [clojure.test :refer :all]
            [duct.core :as duct]
            [integrant.core :as integrant]
            [fx.module.autowire]
            [next.jdbc :as jdbc]
            [clj-http.client :as http]
            [cheshire.core :as cheshire])
  (:import [org.eclipse.jetty.server Server]
           [java.sql Connection]))


(duct/load-hierarchy)


(def valid-config
  {:duct.profile/base  {:duct.core/project-ns 'test}
   :fx.module/autowire {:root 'fx.demo.todo}})


(def todo-table-query
  ["SELECT name FROM sqlite_master WHERE type='table' AND name='todo';"])


(deftest todo-app-test
  (let [config (duct/prep-config valid-config)
        system (integrant/init config)]

    (testing "todo components initialized successfully"
      (let [server     (:fx.demo.todo/server system)
            connection (:fx.demo.todo/db-connection system)
            todo-table (jdbc/execute-one! connection todo-table-query)]
        (is (instance? Server server))
        (is (instance? Connection connection))
        (is (= todo-table {:sqlite_master/name "todo"}))))

    (testing "API handlers works as expected"
      (let [empty-todos-list (-> (http/get "http://localhost:3000/todos")
                                 :body
                                 cheshire/parse-string)]
        (is (seq? empty-todos-list))
        (is (= empty-todos-list [])))

      (let [create-resp (-> (http/post "http://localhost:3000/todos?title=test todo")
                            :body
                            (cheshire/parse-string true)
                            first)]
        (is (= create-resp {:next.jdbc/update-count 1})))

      (let [todo (-> (http/get "http://localhost:3000/todos")
                     :body
                     (cheshire/parse-string true)
                     first)]
        (is (= (:todo/title todo) "test todo"))

        (http/put "http://localhost:3000/todos"
                  {:query-params {:id (:todo/id todo) :done true}}))

      (let [new-todo (-> (http/get "http://localhost:3000/todos")
                         :body
                         (cheshire/parse-string true)
                         first)]
        (is (= (:todo/done new-todo) 1))))

    (integrant/halt! system)))

