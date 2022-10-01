(ns fx.migrate-test
  (:require
   [clojure.test :refer :all]
   [fx.migrate :as sut]
   [fx.containers.postgres :as pg]
   [fx.entity :as entity]
   [fx.repo :as fx.repo]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [malli.instrument :as mi]
   [medley.core :as mdl]
   [clojure.java.io :as io]
   [duct.core :as duct]
   [integrant.core :as ig])
  (:import
   [java.time Clock Instant ZoneOffset]))


(duct/load-hierarchy)
(mi/instrument!)


(def user-schema
  [:spec {:table "user"}
   [:id {:identity true} uuid?]
   [:name [:string {:max 250}]]])


(def modified-user-schema
  [:spec {:table "user"}
   [:id {:identity true} string?] ;; uuid? -> string?
   ; [:name [:string {:max 100}]]     ;; deleted
   [:email string?]]) ;; added


(def modified-user-name-schema
  [:spec {:table "user"}
   [:id {:identity true} uuid?]])
; [:name [:string {:max 250}]]


(defn get-columns [ds]
  (jdbc/execute!
   ds
   ["SELECT *
     FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'user';"]
   {:return-keys true
    :builder-fn  jdbc.rs/as-unqualified-kebab-maps}))


(defn get-table-columns-names [ds]
  (->> (get-columns ds)
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
  (pg/with-datasource ds
    (jdbc/execute! ds [user-sql])

    (let [result    (sut/extract-db-columns ds "user")
          id-column (get result "id")]
      (is (contains? result "id"))
      (is (contains? result "name"))
      (is (contains? result "last_name"))

      (is (map? id-column))
      (is (contains? id-column :pk-name))
      (is (= "uuid" (get id-column :type-name))))))


(deftest get-db-columns-test
  (pg/with-datasource ds
    (jdbc/execute! ds [user-sql])

    (let [result (sut/get-db-columns ds "user")]
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
  (let [user-spec (-> user-schema entity/prepare-spec :spec)
        user      (entity/create-entity :some/test-user user-spec)]
    (testing "column added"
      (is (= (sut/prep-changes user
                               {:id {:type :uuid}}
                               {:id   {:type :uuid}
                                :name {:type :string}})
             {:rollbacks '({:drop-column :name})
              :updates   '({:add-column [:name :string [:not nil]]})})))

    (testing "column deleted"
      (is (= (sut/prep-changes user
                               {:id   {:type :uuid}
                                :name {:type :string}}
                               {:id {:type :uuid}})
             {:rollbacks '({:add-column [:name :string [:not nil]]})
              :updates   '({:drop-column :name})})))

    (testing "column modified"
      (is (= (sut/prep-changes user
                               {:id {:type :uuid :optional true}}
                               {:id {:type :string :primary-key? true}})
             {:rollbacks '({:drop-index [:primary-key :id]}
                           {:alter-column [:id :type :uuid]}
                           {:alter-column [:id :set [:not nil]]})
              :updates   '({:alter-column [:id :type :string]}
                           {:add-index [:primary-key :id]}
                           {:alter-column [:id :drop [:not nil]]})})))

    (testing "tables identical"
      (let [{:keys [rollbacks updates]}
            (sut/prep-changes user
                              {:id {:type :uuid}}
                              {:id {:type :uuid}})]
        (is (empty? updates)
            (empty? rollbacks))))))


(deftest apply-migrations-test
  (pg/with-datasource ds
    (let [user-spec (-> user-schema entity/prepare-spec :spec)
          entity    (entity/create-entity :some/test-user user-spec)]

      (testing "table create"
        (sut/apply-migrations! {:database ds
                                :entities #{entity}})

        (is (sut/table-exist? ds "user"))
        (is (= #{"id" "name"}
               (->> (get-columns ds)
                    (map :column-name)
                    set)))))

    (testing "table alter"
      (let [modified-user-spec (-> modified-user-schema entity/prepare-spec :spec)
            entity             (entity/create-entity :some/test-user modified-user-spec)]
        (sut/apply-migrations! {:database ds
                                :entities #{entity}})
        (let [columns   (get-columns ds)
              id-column (mdl/find-first #(= "id" (:column-name %)) columns)]
          (is (= #{"id" "email"}
                 (->> columns
                      (map :column-name)
                      set)))
          (is (= "varchar" (:udt-name id-column))))))))


(deftest drop-migrations-test
  (pg/with-datasource ds
    (let [entity-spec (-> user-schema entity/prepare-spec :spec)
          entity      (entity/create-entity :some/test-user entity-spec)
          {:keys [rollback-migrations]} (sut/apply-migrations! {:database ds
                                                                :entities #{entity}})]
      (is (sut/table-exist? ds "user"))

      (testing "partial rollback"
        (is (-> (get-table-columns-names ds)
                (contains? "name"))
            "name field should be in DB")

        (let [modified-spec (-> modified-user-name-schema entity/prepare-spec :spec)
              entity        (entity/create-entity :some/test-user modified-spec)
              {:keys [rollback-migrations]} (sut/apply-migrations! {:database ds
                                                                    :entities #{entity}})]
          (is (not (-> (get-table-columns-names ds)
                       (contains? "name")))
              "after running migrations name field shouldn't exist")

          (sut/drop-migrations! ds rollback-migrations)

          (is (-> (get-table-columns-names ds)
                  (contains? "name"))
              "name field should be created again")))

      (is (sut/table-exist? ds "user"))

      (testing "table should be dropped"
        (sut/drop-migrations! ds rollback-migrations)
        (is (not (sut/table-exist? ds "user")))))))


(deftest validate-schema-test
  (pg/with-datasource ds
    (let [entity-spec (-> user-schema entity/prepare-spec :spec)
          entity      (entity/create-entity :some/test-user entity-spec)]
      (is (false? (sut/validate-schema! {:database ds
                                         :entities #{entity}}))
          "user doesn't created yet")

      (sut/apply-migrations! {:database ds
                              :entities #{entity}})

      (is (sut/validate-schema! {:database ds
                                 :entities #{entity}})
          "db schema and entity should match"))))


(deftest store-migrations-test
  (pg/with-datasource ds
    (let [entity-spec (-> user-schema entity/prepare-spec :spec)
          entity      (entity/create-entity :some/test-user entity-spec)
          fixed-clock (Clock/fixed (Instant/now) ZoneOffset/UTC)]
      (testing "migration file should be created"
        (sut/store-migrations! {:database ds
                                :entities #{entity}
                                :clock    fixed-clock})
        (let [path      (format "resources/migrations/%d-%s-%s.edn" (.millis fixed-clock) "some" "test-user")
              migration (io/file path)]
          (is (.exists migration))

          (io/delete-file migration)
          (io/delete-file (io/file "resources/migrations"))))

      (testing "migration file should follow the path pattern"
        (sut/store-migrations! {:database     ds
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



(def entity-w-wrapped-fields
  [:spec {:table "user"}
   [:id {:identity true} uuid?]
   [:column {:wrap? true} [:string {:max 250}]]])


(deftest wrapped-fields-test
  (testing "wrapped fields returned as raw honey sql operator"
    (let [user-spec (-> entity-w-wrapped-fields entity/prepare-spec :spec)
          user      (entity/create-entity :wrapped/user user-spec)]
      (is (= (sut/prep-changes user
                               {:id {:type :uuid}}
                               {:id     {:type :uuid}
                                :column {:type [:string 250]}})
             {:rollbacks '({:drop-column [:quote :column]})
              :updates   '({:add-column [[:quote :column] [:string 250] [:not nil]]})})))))



(def config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.migrate-test}
   :fx.module/repo                    {:migrate {:strategy :update-drop}}
   :fx.containers.postgres/connection {}})


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:identity true} :uuid]
   [:name :string]])


(def ^{:fx/autowire :fx/entity} post
  [:spec {:table "post"}
   [:id {:identity true} :uuid]
   [:created-by {:rel-type :many-to-one} ::user]
   [:current-version {:rel-type :many-to-one} ::post-version]
   [:versions {:rel-type :one-to-many} ::post-version]])


(def ^{:fx/autowire :fx/entity} post-version
  [:spec
   [:id {:identity true} :uuid]
   [:title :string]
   [:content :string]
   [:updated-by {:rel-type :one-to-one} ::user]])


(deftest entities-without-table-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (testing "entities w/o table property in spec doesn't have table in the database"
      (let [all-entities (set (map val (ig/find-derived system :fx/entity)))
            user         (val (ig/find-derived-1 system ::user))
            post         (val (ig/find-derived-1 system ::post))
            ds           (val (ig/find-derived-1 system :fx.database/connection))]
        (is (= (sut/clean-up-entities all-entities)
               (list user post))
            "post-version entity excluded")

        (is (not (sut/table-exist? ds "post_version")))))

    (testing "entities w/o table property saved as json as part of the parent entity"
      (let [user         (val (ig/find-derived-1 system ::user))
            post         (val (ig/find-derived-1 system ::post))

            user-id      (random-uuid)
            post-id      (random-uuid)
            version-2-id (random-uuid)
            version-1-id (random-uuid)

            version-1    {:id         version-1-id
                          :title      "First title"
                          :content    "Content"
                          :updated-by user-id}
            version-2    {:id         version-2-id
                          :title      "Second title"
                          :content    "New Content"
                          :updated-by user-id}]

        (fx.repo/save! user {:id   user-id
                             :name "Jack"})

        (fx.repo/save! post {:id              post-id
                             :created-by      user-id
                             :current-version version-1
                             :versions        [version-1]})

        (let [result (fx.repo/find! post {:id post-id})]
          (is (some? result))
          (is (= (str version-1-id)
                 (get-in result [:current-version :id]))))

        (fx.repo/update! post
                         {:current-version version-2
                          :versions        [version-2 version-1]}
                         {:id post-id})

        (let [result (fx.repo/find! post {:id post-id})]
          (is (= 2 (count (:versions result))))
          (is (= (str version-2-id)
                 (get-in result [:current-version :id]))))

        (testing "crud functions can recognize entities w/o table property and build valid queries"
          (let [result (fx.repo/find! post {:id     post-id
                                            :nested true})]
            (is (= version-2-id (get-in result [:current-version :id])))
            (is (= user-id (get-in result [:current-version :updated-by])))))))


    (ig/halt! system)))
