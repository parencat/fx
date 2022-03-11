(ns dev
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.namespace.repl :as namespace.repl]
            [clojure.java.io :as io]
            [duct.core :as duct]
            [duct.core.repl]
            [eftest.runner :as eftest]
            [integrant.core :as ig]
            [integrant.repl :refer [clear halt go init prep reset set-prep!]]
            [integrant.repl.state :refer [config system]]))


(duct/load-hierarchy)

(defn read-config []
  (duct/read-config (io/resource "todo/config.edn")))

(defn test []
  (eftest/run-tests (eftest/find-tests "test")))

(def profiles
  [:duct.profile/dev :duct.profile/local])

(namespace.repl/set-refresh-dirs "dev/src" "src" "test")

(when (io/resource "local.clj")
  (load "local"))

(set-prep!
 #(duct/prep-config (read-config) profiles))


(comment
 (go)
 (reset)
 (halt)

 (test)

 nil)
