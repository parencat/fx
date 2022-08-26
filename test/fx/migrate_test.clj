(ns fx.migrate-test
  (:require
   [clojure.test :refer :all]
   [fx.migrate :as sut]
   [fx.containers.postgres :as pg]
   [fx.entity :as entity]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [malli.instrument :as mi]
   [medley.core :as mdl]
   [clojure.java.io :as io])
  (:import
   [java.sql Connection]
   [java.time Clock Instant ZoneOffset]))


(mi/instrument!)


(def user-schema
  [:spec {:table "user"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]])


(def modified-user-schema
  [:spec {:table "user"}
   [:id {:primary-key? true} string?] ;; uuid? -> string?
   ; [:name [:string {:max 100}]]     ;; deleted
   [:email string?]]) ;; added


(def modified-user-name-schema
  [:spec {:table "user"}
   [:id {:primary-key? true} uuid?]])
; [:name [:string {:max 250}]]


(defn get-columns [connection]
  (jdbc/execute!
   connection
   ["SELECT *
     FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'user';"]
   {:return-keys true
    :builder-fn  jdbc.rs/as-unqualified-kebab-maps}))


(defn get-table-columns-names [connection]
  (->> (get-columns connection)
       (map :column-name)
       set))


(def user-sql
  "create table \"user\"
    (
      id        uuid    not null primary key,
      name      varchar(250) not null,
      last_name varchar
    )")


(deftest extract-db-columns-test
  (pg/with-connection connection
    (jdbc/execute! connection [user-sql])

    (let [result    (sut/extract-db-columns connection "user")
          id-column (get result "id")]
      (is (contains? result "id"))
      (is (contains? result "name"))
      (is (contains? result "last_name"))

      (is (map? id-column))
      (is (contains? id-column :pk-name))
      (is (= "uuid" (get id-column :type-name))))))


(deftest get-db-columns-test
  (pg/with-connection connection
    (jdbc/execute! connection [user-sql])

    (let [result (sut/get-db-columns connection "user")]
      (is (contains? result :id))
      (is (contains? result :name))

      (is (= {:id        {:type :uuid :primary-key? true}
              :last-name {:type :varchar :optional true}
              :name      {:type [:varchar 250]}}
             result)))))


(deftest extract-entity-columns-test
  (let [user-spec (-> user-schema entity/prepare-spec :spec)
        user      (entity/create-entity :some/test-user user-spec)
        result    (sut/get-entity-columns user)]
    (is (= {:id   {:type :uuid :primary-key? true}
            :name {:type [:varchar 250]}}
           result))))


(deftest prep-changes-test
  (testing "column added"
    (is (= (sut/prep-changes {:id {:type :uuid}}
                             {:id   {:type :uuid}
                              :name {:type :string}})
           {:rollbacks '({:drop-column :name})
            :updates   '({:add-column [:name :string [:not nil]]})})))

  (testing "column deleted"
    (is (= (sut/prep-changes {:id   {:type :uuid}
                              :name {:type :string}}
                             {:id {:type :uuid}})
           {:rollbacks '({:add-column [:name :string [:not nil]]})
            :updates   '({:drop-column :name})})))

  (testing "column modified"
    (is (= (sut/prep-changes {:id {:type :uuid :optional true}}
                             {:id {:type :string :primary-key? true}})
           {:rollbacks '({:drop-index [:primary-key :id]}
                         {:alter-column [:id :type :uuid]}
                         {:alter-column [:id :set [:not nil]]})
            :updates   '({:alter-column [:id :type :string]}
                         {:add-index [:primary-key :id]}
                         {:alter-column [:id :drop [:not nil]]})})))

  (testing "tables identical"
    (let [{:keys [rollbacks updates]}
          (sut/prep-changes {:id {:type :uuid}}
                            {:id {:type :uuid}})]
      (is (empty? updates)
          (empty? rollbacks)))))


(deftest apply-migrations-test
  (pg/with-connection connection
    (let [user-spec (-> user-schema entity/prepare-spec :spec)
          entity    (entity/create-entity :some/test-user user-spec)]

      (testing "table create"
        (sut/apply-migrations! {:database connection
                                :entities #{entity}})

        (is (sut/table-exist? connection "user"))
        (is (= #{"id" "name"}
               (->> (get-columns connection)
                    (map :column-name)
                    set)))))

    (testing "table alter"
      (let [modified-user-spec (-> modified-user-schema entity/prepare-spec :spec)
            entity             (entity/create-entity :some/test-user modified-user-spec)]
        (sut/apply-migrations! {:database connection
                                :entities #{entity}})
        (let [columns   (get-columns connection)
              id-column (mdl/find-first #(= "id" (:column-name %)) columns)]
          (is (= #{"id" "email"}
                 (->> columns
                      (map :column-name)
                      set)))
          (is (= "varchar" (:udt-name id-column))))))))


(deftest drop-migrations-test
  (pg/with-connection connection
    (let [entity-spec (-> user-schema entity/prepare-spec :spec)
          entity      (entity/create-entity :some/test-user entity-spec)
          {:keys [rollback-migrations]} (sut/apply-migrations! {:database connection
                                                                :entities #{entity}})]
      (is (sut/table-exist? connection "user"))

      (testing "partial rollback"
        (is (-> (get-table-columns-names connection)
                (contains? "name"))
            "name field should be in DB")

        (let [modified-spec (-> modified-user-name-schema entity/prepare-spec :spec)
              entity        (entity/create-entity :some/test-user modified-spec)
              {:keys [rollback-migrations]} (sut/apply-migrations! {:database connection
                                                                    :entities #{entity}})]
          (is (not (-> (get-table-columns-names connection)
                       (contains? "name")))
              "after running migrations name field shouldn't exist")

          (sut/drop-migrations! connection rollback-migrations)

          (is (-> (get-table-columns-names connection)
                  (contains? "name"))
              "name field should be created again")))

      (is (sut/table-exist? connection "user"))

      (testing "table should be dropped"
        (sut/drop-migrations! connection rollback-migrations)
        (is (not (sut/table-exist? connection "user")))))))


(deftest validate-schema-test
  (pg/with-connection connection
    (let [entity-spec (-> user-schema entity/prepare-spec :spec)
          entity      (entity/create-entity :some/test-user entity-spec)]
      (is (false? (sut/validate-schema! {:database connection
                                         :entities #{entity}}))
          "user doesn't created yet")

      (sut/apply-migrations! {:database connection
                              :entities #{entity}})

      (is (sut/validate-schema! {:database connection
                                 :entities #{entity}})
          "db schema and entity should match"))))


(deftest store-migrations-test
  (pg/with-connection connection
    (let [entity-spec (-> user-schema entity/prepare-spec :spec)
          entity      (entity/create-entity :some/test-user entity-spec)
          fixed-clock (Clock/fixed (Instant/now) ZoneOffset/UTC)]
      (testing "migration file should be created"
        (sut/store-migrations! {:database connection
                                :entities #{entity}
                                :clock    fixed-clock})
        (let [path      (format "resources/migrations/%d-%s-%s.edn" (.millis fixed-clock) "some" "test-user")
              migration (io/file path)]
          (is (.exists migration))

          (io/delete-file migration)
          (io/delete-file (io/file "resources/migrations"))))

      (testing "migration file should follow the path pattern"
        (sut/store-migrations! {:database     connection
                                :entities     #{entity}
                                :clock        fixed-clock
                                :path-pattern "resources/some/${entity}/${number}.edn"
                                :path-params  {:number 123}})
        (let [path      "resources/some/test-user/123.edn"
              migration (io/file path)]
          (is (.exists migration))

          (io/delete-file migration)
          (io/delete-file (io/file "resources/some/test-user"))
          (io/delete-file (io/file "resources/some")))))))


(deftest interpolate-test
  (are [test result] (= result (sut/interpolate (first test) (second test)))
    ["my name is ${name}" {:name "John"}] "my name is John"
    ["${name}" {:name "John"}] "John"
    ["/${host}:${port}/path" {:host "www.test.com" :port 8080}] "/www.test.com:8080/path"
    ["resources/${folder}/${name}.edn" {:folder "migrations" :name "user"}] "resources/migrations/user.edn"))
