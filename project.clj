(defproject io.github.parencat/fx "0.1.4-SNAPSHOT"
  :description "Set of Duct modules for rapid clojure development"
  :url "https://github.com/parencat/fx"

  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.namespace "1.3.0"]
                 [org.clojure/java.classpath "1.0.0"]
                 [org.clojure/core.match "1.0.0"]
                 [duct/core "0.8.0"]
                 [integrant "0.8.0"]
                 [weavejester/dependency "0.2.1"]
                 [medley "1.4.0"]
                 [metosin/malli "0.9.2"]
                 [differ "0.3.3"]
                 [com.github.seancorfield/next.jdbc "1.3.847"]
                 [com.github.seancorfield/honeysql "2.4.947"]
                 [org.postgresql/postgresql "42.5.0"]
                 [hikari-cp "3.0.0"]
                 [clj-test-containers "0.7.4"]
                 [org.testcontainers/postgresql "1.17.6"]
                 [com.cnuernber/charred "1.014"]]

  :profiles
  {:dev {:source-paths ["dev"]
         :dependencies [[vvvvalvalval/scope-capture "0.3.3-s1"]
                        [djblue/portal "0.34.2"]]}}

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
