(ns fx.database-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [fx.containers.postgres :as pg]
   [malli.instrument :as mi])
  (:import
   [java.sql Connection]))


(duct/load-hierarchy)
(mi/instrument!)


(def invalid-config
  {:duct.profile/base
   {:duct.core/project-ns   'test
    :fx.database/connection {}}})


(deftest database-bad-config-test
  (is (thrown? Exception
               (-> invalid-config
                   (duct/prep-config)
                   (ig/init)))))


(deftest database-url-config-test
  (let [container        (pg/pg-container {:port 5432})
        port             (get (:mapped-ports container) 5432)
        host             (:host container)
        user             (.getUsername (:container container))
        password         (.getPassword (:container container))
        url              (str "jdbc:postgresql://" host ":" port "/test?user=" user "&password=" password)
        valid-url-config {:duct.profile/base
                          {:duct.core/project-ns   'test
                           :fx.containers/postgres {:container container}
                           :fx.database/connection {:url url}}}
        config           (duct/prep-config valid-url-config)
        system           (ig/init config)
        connection       (:fx.database/connection system)]

    (is (instance? Connection connection))

    (ig/halt! system)))


(deftest database-map-config-test
  (let [container        (pg/pg-container {:port 5432})
        port             (get (:mapped-ports container) 5432)
        host             (:host container)
        user             (.getUsername (:container container))
        password         (.getPassword (:container container))
        valid-url-config {:duct.profile/base
                          {:duct.core/project-ns   'test
                           :fx.containers/postgres {:container container}
                           :fx.database/connection {:user   user :password password
                                                    :dbtype "postgres"
                                                    :host   host
                                                    :dbname "test"
                                                    :port   port}}}
        config           (duct/prep-config valid-url-config)
        system           (ig/init config)
        connection       (:fx.database/connection system)]

    (is (instance? Connection connection))

    (ig/halt! system)))
