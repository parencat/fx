(defproject fx/modules "0.1.0"
  :description "Set of Duct modules for rapid clojure development"
  :url "https://github.com/Minoro-Ltd/fx-demo"

  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.namespace "1.2.0"]
                 [org.clojure/java.classpath "1.0.0"]
                 [duct/core "0.8.0"]
                 [integrant "0.8.0"]]

  :profiles
  {:dev {:dependencies [[vvvvalvalval/scope-capture "0.3.2"]]}})
