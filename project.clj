(defproject fx-demo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [duct/core "0.8.0"]
                 [org.clojure/java.classpath "1.0.0"]
                 [ring "1.9.5"]
                 [compojure "1.6.2"]
                 [com.github.seancorfield/honeysql "2.2.868"]
                 [com.github.seancorfield/next.jdbc "1.2.772"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]
                 [ring/ring-json "0.5.1"]]

  :plugins [[duct/lein-duct "0.12.3"]]
  :middleware [lein-duct.plugin/middleware]

  :main ^:skip-aot fx-demo.main
  :resource-paths ["resources" "target/resources"]

  :prep-tasks ["javac" "compile" ["run" ":duct/compiler"]]

  :profiles
  {:dev     {:source-paths   ["dev/src"]
             :resource-paths ["dev/resources"]
             :dependencies   [[integrant/repl "0.3.2"]
                              [hawk "0.2.11"]
                              [eftest "0.5.9"]]}

   :repl    {:prep-tasks   ^:replace ["javac" "compile"]
             :repl-options {:init-ns user}}

   :uberjar {:aot :all}})
