(defproject org.clojars.tka4enko/fx "0.1.1-SNAPSHOT"
  :description "Set of Duct modules for rapid clojure development"
  :url "https://github.com/Minoro-Ltd/fx-demo"

  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.namespace "1.3.0"]
                 [org.clojure/java.classpath "1.0.0"]
                 [org.clojure/core.match "1.0.0"]
                 [duct/core "0.8.0"]
                 [integrant "0.8.0"]
                 [weavejester/dependency "0.2.1"]
                 [metosin/malli "0.8.4"]
                 [medley "1.4.0"]
                 [com.github.seancorfield/next.jdbc "1.2.780"]
                 [com.github.seancorfield/honeysql "2.2.891"]
                 [org.postgresql/postgresql "42.3.4"]]

  :profiles
  {:dev {:dependencies [[vvvvalvalval/scope-capture "0.3.3-s1"]
                        [clj-test-containers "0.7.0"]
                        [org.testcontainers/postgresql "1.17.1"]]}}

  :deploy-repositories
  [["clojars" {:sign-releases false
               :url           "https://clojars.org/repo"
               :username      :env/CLOJARS_USERNAME
               :password      :env/CLOJARS_PASSWORD}]]

  :release-tasks
  [["vcs" "assert-committed"]
   ["change" "version" "leiningen.release/bump-version" "release"]
   ["vcs" "commit"]
   ["vcs" "tag" "--no-sign"]
   ["deploy" "clojars"]
   ["change" "version" "leiningen.release/bump-version"]
   ["vcs" "commit"]
   ["vcs" "push"]])
