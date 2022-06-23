(ns fx.migrate-test
  (:require
   [clojure.test :refer :all]
   [fx.migrate :as sut]
   [fx.containers.postgres :as pg]
   [fx.entity :as entity]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [honey.sql :as sql]))


(mi/instrument!)

(deftest database-url-config-test
  (let [container        (pg/pg-container {:port 5432})
        port             (get (:mapped-ports container) 5432)
        host             (:host container)
        user             (.getUsername (:container container))
        password         (.getPassword (:container container))
        url              (str "jdbc:postgresql://" host ":" port "/test?user=" user "&password=" password)
        connection       (jdbc/get-connection {:jdbcUrl url})
        entity-schema    [:spec {:table "user"}
                          [:id {:primary-key? true} uuid?]
                          [:name [:string {:max 250}]]]
        entity           (entity/create-entity
                           :test-user
                           {:spec (-> entity-schema entity/prepare-spec :spec)
                            :database connection})]
    
    (testing "table create"
      (sut/apply-migrations {:database connection
                             :entities [entity]})
      (is (sut/table-exist? connection "user"))
      
      (is (= #{"id" "name"}
             (->> (jdbc/execute! connection
                    ["SELECT *
                      FROM information_schema.columns
                      WHERE table_schema = 'public' AND table_name = 'user';"]
                    {:return-keys true
                     :builder-fn  jdbc.rs/as-unqualified-kebab-maps})
                  (map :column-name)
                  set))))

    (testing "table alter"
      (let [entity-schema    [:spec {:table "user"}
                              [:id {:primary-key? true} string?] ;; uuid? -> string?
                              ; [:name [:string {:max 100}]]     ;; deleted
                              [:email string?]]                  ;; added
            entity           (entity/create-entity
                               :test-user
                               {:spec (-> entity-schema entity/prepare-spec :spec)
                                :database connection})]
        (sut/apply-migrations {:database connection
                               :entities [entity]})
        (prn (jdbc/execute! connection
                    ["SELECT *
                      FROM information_schema.columns
                      WHERE table_schema = 'public' AND table_name = 'user';"]
                    {:return-keys true
                     :builder-fn  jdbc.rs/as-unqualified-kebab-maps}))
        ))

    (pg/stop container)))

(comment
  (sut/prep-changes [[:id :uuid [:not nil]]]
                    [[:id :string [:not nil]]]))
