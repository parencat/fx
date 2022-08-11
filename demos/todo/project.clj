(defproject todo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [duct/core "0.8.0"]
                 [org.clojars.tka4enko/fx "0.1.1"]
                 [ring "1.9.5"]
                 [ring/ring-json "0.5.1"]
                 [compojure "1.6.3"]
                 [com.github.seancorfield/next.jdbc "1.2.796"]
                 [com.github.seancorfield/honeysql "2.3.911"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]]

  :plugins [[duct/lein-duct "0.12.3"]]
  :middleware [lein-duct.plugin/middleware]

  :resource-paths ["resources"]

  :profiles
  {:dev     {:source-paths   ["dev/src"]
             :resource-paths ["dev/resources"]
             :dependencies   [[integrant/repl "0.3.2"]
                              [hawk "0.2.11"]
                              [eftest "0.5.9"]
                              [clj-http "3.12.3"]
                              [cheshire "5.11.0"]]}

   :uberjar {:aot :all}})
