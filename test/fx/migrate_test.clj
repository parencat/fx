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


(def address-schema
  [:spec {:table "address"}
   [:id {:identity true} uuid?]
   [:users {:rel-type :one-to-many} :some/test-user]])


(def modified-address-schema
  [:spec {:table "address"}
   [:id {:identity true} uuid?]
   [:users {:rel-type :many-to-many
            :join     :some/address-users} :some/test-user]])


(def address-users-schema
  [:spec {:table    "address_users"
          :identity [:user-id :address-id]}
   [:user-id {:rel-type :many-to-one
              :cascade  true} :some/test-user]
   [:address-id {:rel-type :many-to-one
                 :cascade  true} :some/test-address]])


(def person-schema
  [:spec {:table "person"}
   [:id {:identity true} :uuid]
   [:user {:wrap true} :integer]])


(def modified-person-schema
  [:spec {:table "person"}
   [:id {:identity true} :uuid]
   [:user {:wrap true} [:string {:max 250}]]])


(defn get-columns
  ([ds]
   (get-columns ds "user"))
  ([ds table]
   (let [query (format "SELECT * FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = '%s';"
                       table)]
     (jdbc/execute! ds [query]
       {:return-keys true
        :builder-fn  jdbc.rs/as-unqualified-kebab-maps}))))


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

      (is (= {:id        {:type :uuid :primary-key true}
              :last-name {:type :varchar :optional true}
              :name      {:type [:varchar 250]}}
             result)))))


(deftest extract-entity-columns-test
  (let [user-spec (-> user-schema entity/prepare-spec :spec)
        user      (entity/create-entity :some/test-user user-spec)
        result    (sut/get-entity-columns user)]
    (is (= {:id   {:type :uuid :primary-key true}
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
              :updates   '({:add-column-raw [:name :string [:not nil]]})})))

    (testing "column deleted"
      (is (= (sut/prep-changes user
                               {:id   {:type :uuid}
                                :name {:type :string}}
                               {:id {:type :uuid}})
             {:rollbacks '({:add-column-raw [:name :string [:not nil]]})
              :updates   '({:drop-column :name})})))

    (testing "column modified"
      (is (= (sut/prep-changes user
                               {:id {:type :uuid :optional true}}
                               {:id {:type :string :primary-key true}})
             {:rollbacks '({:drop-index [:primary-key :id]}
                           {:alter-column-raw [:id :type :uuid]}
                           {:alter-column-raw [:id :set [:not nil]]})
              :updates   '({:alter-column-raw [:id :type :string]}
                           {:add-index [:primary-key :id]}
                           {:alter-column-raw [:id :drop [:not nil]]})})))

    (testing "tables identical"
      (let [{:keys [rollbacks updates]}
            (sut/prep-changes user
                              {:id {:type :uuid}}
                              {:id {:type :uuid}})]
        (is (empty? updates)
            (empty? rollbacks))))))


(deftest apply-migrations-test
  (pg/with-datasource ds
    (let [user-spec      (-> user-schema entity/prepare-spec :spec)
          user-entity    (entity/create-entity :some/test-user user-spec)
          address-spec   (-> address-schema entity/prepare-spec :spec)
          address-entity (entity/create-entity :some/test-address address-spec)
          person-spec    (-> person-schema entity/prepare-spec :spec)
          person-entity  (entity/create-entity :some/test-person person-spec)]

      (testing "table create"
        (sut/apply-migrations! {:database ds
                                :entities #{user-entity address-entity person-entity}})

        (is (sut/table-exist? ds "user"))
        (is (sut/table-exist? ds "address"))

        (is (= #{"id" "name"}
               (->> (get-columns ds "user")
                    (map :column-name)
                    set)))
        (is (= #{"id"}
               (->> (get-columns ds "address")
                    (map :column-name)
                    set)))))

    (testing "table alter"
      (let [user-spec      (-> modified-user-schema entity/prepare-spec :spec)
            user-entity    (entity/create-entity :some/test-user user-spec)
            address-spec   (-> modified-address-schema entity/prepare-spec :spec)
            address-entity (entity/create-entity :some/test-address address-spec)]

        (sut/apply-migrations! {:database ds
                                :entities #{user-entity address-entity}})

        (let [columns   (get-columns ds)
              id-column (mdl/find-first #(= "id" (:column-name %)) columns)]
          (is (= #{"id" "email"}
                 (->> columns (map :column-name) set)))
          (is (= "varchar" (:udt-name id-column)))))

      (testing "table alter with wrapped column name"
        (let [person-spec   (-> modified-person-schema entity/prepare-spec :spec)
              person-entity (entity/create-entity :some/test-person person-spec)]

          (let [columns     (get-columns ds "person")
                user-column (mdl/find-first #(= "user" (:column-name %)) columns)]
            (is (= "int4" (:udt-name user-column))))

          (sut/apply-migrations! {:database ds
                                  :entities #{person-entity}})

          (let [columns     (get-columns ds "person")
                user-column (mdl/find-first #(= "user" (:column-name %)) columns)]
            (is (= "varchar" (:udt-name user-column)))))


        ;; won't work for now
        ;; requires a very intelligent workaround based on
        ;; finding all foreign key constraints, dropping them
        ;; and recreating them back
        ;; https://zauner.nllk.net/post/0036-change-primary-key-of-existing-postgresql-table/

        ;; e.g.
        ;; SELECT
        ;;     tc.table_schema,
        ;;     tc.constraint_name,
        ;;     tc.table_name,
        ;;     kcu.column_name,
        ;;     ccu.table_schema AS foreign_table_schema,
        ;;     ccu.table_name AS foreign_table_name,
        ;;     ccu.column_name AS foreign_column_name
        ;; FROM
        ;;     information_schema.table_constraints AS tc
        ;;     JOIN information_schema.key_column_usage AS kcu
        ;;         ON tc.constraint_name = kcu.constraint_name
        ;;         AND tc.table_schema = kcu.table_schema
        ;;     JOIN information_schema.constraint_column_usage AS ccu
        ;;         ON ccu.constraint_name = tc.constraint_name
        ;;         AND ccu.table_schema = tc.table_schema
        ;; WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name ='order';

        ;; and then
        ;; ALTER TABLE "order"
        ;; DROP CONSTRAINT order_pkey CASCADE,
        ;; ADD PRIMARY KEY(column_i_want_to_use_as_a_pkey_now);

        ;; and then recreate FKs on all tables from step one

        #_(testing "join table changed on owning entity changes"
            (let [join-spec   (-> address-users-schema entity/prepare-spec :spec)
                  join-entity (entity/create-entity :some/address-users join-spec)]
              (sut/apply-migrations! {:database ds
                                      :entities #{user-entity address-entity join-entity}})

              (is (sut/table-exist? ds "address_users"))

              (let [columns   (get-columns ds "address_users")
                    id-column (mdl/find-first #(= "user_id" (:column-name %)) columns)]
                (is (= "uuid" (:udt-name id-column))))

              (sut/apply-migrations! {:database ds
                                      :entities #{changed-user-entity changed-address-entity join-entity}})

              (let [columns   (get-columns ds "address_users")
                    id-column (mdl/find-first #(= "user_id" (:column-name %)) columns)]
                (is (= "varchar" (:udt-name id-column))))))))))


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
   [:column {:wrap true} [:string {:max 250}]]])


(deftest wrapped-fields-test
  (testing "wrapped fields returned as raw honey sql operator"
    (let [user-spec (-> entity-w-wrapped-fields entity/prepare-spec :spec)
          user      (entity/create-entity :wrapped/user user-spec)]
      (is (= (sut/prep-changes user
                               {:id {:type :uuid}}
                               {:id     {:type :uuid}
                                :column {:type [:string 250]}})
             {:rollbacks '({:drop-column [:quote :column]})
              :updates   '({:add-column-raw [[:quote :column] [:string 250] [:not nil]]})})))))



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
   [:current-version {:rel-type :one-to-one} ::post-version]
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
            post-version (val (ig/find-derived-1 system ::post-version))
            ds           (val (ig/find-derived-1 system :fx.database/connection))]
        (is (= -1 (.indexOf (sut/clean-up-entities all-entities) post-version))
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
          (let [result (fx.repo/find! post {:id             post-id
                                            :fx.repo/nested true})]
            (is (= version-2-id (get-in result [:current-version :id])))
            (is (= user-id (get-in result [:current-version :updated-by])))))))


    (ig/halt! system)))


(def ^{:fx/autowire :fx/entity} student
  [:spec {:table "student"}
   [:id {:identity true} :uuid]
   [:name :string]
   [:courses {:rel-type :many-to-many
              :join     ::student-course} ::course]])


(def ^{:fx/autowire :fx/entity} course
  [:spec {:table "course"}
   [:id {:identity true} :uuid]
   [:title :string]
   [:students {:rel-type :many-to-many
               :join     ::student-course} ::student]])


(def ^{:fx/autowire :fx/entity} student-course
  [:spec {:table    "student_course"
          :identity [:student :course]}
   [:student {:rel-type :many-to-one
              :cascade  true} ::student]
   [:course {:rel-type :many-to-one
             :cascade  true} ::course]])


(deftest join-table-test
  (let [config (duct/prep-config config)
        system (ig/init config)
        ds     (val (ig/find-derived-1 system :fx.database/connection))]

    (is (sut/table-exist? ds "student"))
    (is (sut/table-exist? ds "course"))
    (is (sut/table-exist? ds "student_course"))

    (let [student        (val (ig/find-derived-1 system ::student))
          course         (val (ig/find-derived-1 system ::course))
          student-course (val (ig/find-derived-1 system ::student-course))
          s-id           (random-uuid)
          c1-id          (random-uuid)
          c2-id          (random-uuid)]
      (fx.repo/save! student {:id s-id :name "Student"})
      (fx.repo/save! course {:id c1-id :title "Course 1"})
      (fx.repo/save! course {:id c2-id :title "Course 2"})
      (fx.repo/save! student-course {:student s-id :course c1-id})

      (let [s (fx.repo/find! student {:id s-id :fx.repo/nested true})]
        (is (seq (:courses s)))
        (is (some #(= (:id %) c1-id) (:courses s))
            "some of the courses is a saved course"))

      (let [s  (fx.repo/find-all! student {:fx.repo/nested true})
            cs (fx.repo/find-all! course)]
        (is (= 1 (count s)))
        (is (= 2 (count cs)))
        (is (some #(= (:id %) c1-id) (-> s first :courses))
            "some of the courses is a saved course"))

      (fx.repo/save! student-course {:student s-id :course c2-id})

      (let [s (fx.repo/find! student {:id s-id :fx.repo/nested true})]
        (is (= #{c1-id c2-id}
               (->> (:courses s)
                    (map :id)
                    set))
            "two courses attached to student")))

    (ig/halt! system)))


(def item-schema
  [:spec {:table "item"}
   [:id {:identity true} :serial]])

(def order-schema
  [:spec {:table "order"}
   [:item {:rel-type :one-to-one} ::item]])


(deftest handle-serial-references-test
  (pg/with-datasource ds
    (let [item-spec  (-> item-schema entity/prepare-spec :spec)
          item       (entity/create-entity ::item item-spec)
          order-spec (-> order-schema entity/prepare-spec :spec)
          order      (entity/create-entity ::order order-spec)
          ref-type   (-> (sut/entity->columns-ddl order)
                         first second second)]
      (is (= ref-type :integer))

      (sut/apply-migrations! {:database ds :entities #{item order}})

      (is (= "integer"
             (-> (get-columns ds "order")
                 first
                 :data-type))))))
