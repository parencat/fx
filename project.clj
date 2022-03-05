(defproject org.clojars.tka4enko/fx "0.1.0"
  :description "Set of Duct modules for rapid clojure development"
  :url "https://github.com/Minoro-Ltd/fx-demo"

  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.namespace "1.2.0"]
                 [org.clojure/java.classpath "1.0.0"]
                 [duct/core "0.8.0"]
                 [integrant "0.8.0"]]

  :profiles
  {:dev {:dependencies [[vvvvalvalval/scope-capture "0.3.2"]]}}

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
