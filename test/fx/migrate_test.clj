(ns fx.migrate-test
  (:require
   [clojure.test :refer :all]
   [fx.migrate :as sut]
   [fx.containers.postgres :as pg]
   [fx.entity :as entity]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [malli.instrument :as mi]
   [medley.core :as mdl]))


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


(defn get-columns [connection]
  (->> (jdbc/execute! connection
                      ["SELECT *
                      FROM information_schema.columns
                      WHERE table_schema = 'public' AND table_name = 'user';"]
                      {:return-keys true
                       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))


(deftest database-url-config-test
  (pg/with-connection connection
    (let [entity (entity/create-entity :test-user
                                       {:spec     (-> user-schema entity/prepare-spec :spec)
                                        :database connection})]

      (testing "table create"
        (sut/apply-migrations! {:database connection
                                :entities  [entity]})

        (is (sut/table-exist? connection "user"))
        (is (= #{"id" "name"}
               (->> (get-columns connection)
                    (map :column-name)
                    set)))))

    (testing "table alter"
      (let [entity (entity/create-entity :test-user
                                         {:spec     (-> modified-user-schema entity/prepare-spec :spec)
                                          :database connection})]
        (sut/apply-migrations! {:database connection
                                :entities  [entity]})
        (let [columns   (get-columns connection)
              id-column (mdl/find-first #(= "id" (:column-name %)) columns)]
          (is (= #{"id" "email"}
                 (->> columns
                      (map :column-name)
                      set)))
          (is (= "varchar" (:udt-name id-column))))))))


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
  (let [user   (entity/create-entity
                :test-user
                {:spec     (-> user-schema entity/prepare-spec :spec)
                 :database nil})
        result (sut/get-entity-columns user)]

    (is (= {:id   {:type :uuid :primary-key? true}
            :name {:type [:varchar 250]}}
           result))))


(deftest prep-changes-test
  (testing "column added"
    (is (= (sut/prep-changes {:id {:type :uuid}}
                             {:id   {:type :uuid}
                              :name {:type :string}})
           '({:add-column [:name :string [:not nil]]}))))

  (testing "column deleted"
    (is (= (sut/prep-changes {:id   {:type :uuid}
                              :name {:type :string}}
                             {:id {:type :uuid}})
           '({:drop-column :name}))))

  (testing "column modified"
    (is (= (sut/prep-changes {:id {:type :uuid :optional true}}
                             {:id {:type :string :primary-key? true}})
           '({:alter-column [:id :type :string]}
             {:add-index [:primary-key :id]}
             {:alter-column [:id :drop [:not nil]]}))))

  (testing "tables identical"
    (is (empty? (sut/prep-changes {:id {:type :uuid}}
                                  {:id {:type :uuid}})))))
