(ns fx.data-types-test
  (:require
   [clojure.test :refer :all]
   [fx.system :refer [with-system]]
   [duct.core :as duct]
   [integrant.core :as ig]
   [fx.migrate]
   [fx.repo]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [malli.instrument :as mi]
   [medley.core :as mdl]
   [fx.entity :as entity])
  (:import
   [java.time LocalDateTime OffsetDateTime ZoneOffset LocalDate LocalTime OffsetTime Duration]))


(duct/load-hierarchy)
(mi/instrument!)


(def query-template
  "SELECT * FROM information_schema.columns
   WHERE table_schema = 'public' AND table_name = '%s';")


(defn get-columns [ds table]
  (let [query (format query-template table)]
    (jdbc/execute! ds [query]
      {:return-keys true
       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))


(def config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.data-types-test}
   :fx.module/repo                    {:migrate {:strategy :update-drop}}
   :fx.containers.postgres/connection {}})


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id :uuid]
   [:first-name :string]
   [:last-name string?]
   [:appeal [:char {:max 2}]]
   [:age :smallint]
   [:friends :bigint]
   [:house-index :integer]
   [:height :decimal]
   [:weight [number? {:precision 6 :scale 2}]]
   [:speed :real]
   [:wins-percent :double]
   [:work-id :serial]
   [:active :boolean]
   [:address :jsonb]])


(deftest data-types-mapping-test
  (let [config (duct/prep-config config)
        system (ig/init config)
        ds     (val (ig/find-derived-1 system :fx.database/connection))
        user   (val (ig/find-derived-1 system ::user))]

    (testing "DB table created correctly"
      (let [columns (mdl/index-by :column-name (get-columns ds "user"))]
        (is (fx.migrate/table-exist? ds "user"))

        (is (= "uuid" (get-in columns ["id" :data-type])))
        (is (= "character varying" (get-in columns ["first_name" :data-type])))
        (is (= "character varying" (get-in columns ["last_name" :data-type])))
        (is (= "character" (get-in columns ["appeal" :data-type])))
        (is (= "smallint" (get-in columns ["age" :data-type])))
        (is (= "bigint" (get-in columns ["friends" :data-type])))
        (is (= "integer" (get-in columns ["house_index" :data-type])))
        (is (= "numeric" (get-in columns ["height" :data-type])))
        (is (= "numeric" (get-in columns ["weight" :data-type])))
        (is (= "real" (get-in columns ["speed" :data-type])))
        (is (= "double precision" (get-in columns ["wins_percent" :data-type])))
        (is (= "integer" (get-in columns ["work_id" :data-type])))
        (is (= "boolean" (get-in columns ["active" :data-type])))
        (is (= "jsonb" (get-in columns ["address" :data-type])))))

    (testing "saving entity works correctly"
      (let [user-id (random-uuid)]
        (fx.repo/save! user {:id           user-id
                             :first-name   "Jack"
                             :last-name    "Daniels"
                             :appeal       "Mr"
                             :age          200
                             :friends      (long 91474836475)
                             :house-index  153
                             :height       160.5
                             :weight       82.5
                             :speed        2.123456
                             :wins-percent 80.33987654
                             :active       true
                             :address      {:street  "Lynchburg Hwy"
                                            :house   280
                                            :city    "Lynchburg"
                                            :country "US"}})

        (let [{:keys [address work-id friends age speed]}
              (fx.repo/find! user {:id user-id})]
          (is (some? work-id)
              "serial field generated")

          (is (instance? Long friends))
          (is (int? age))
          (is (float? speed))

          (is (map? address))
          (is (= 280 (:house address))))))

    (ig/halt! system)))


(def ^{:fx/autowire :fx/entity} chip
  [:spec {:enum   "laptop_chips"
          :values ["intel" "amd" "m1"]}])


(def ^{:fx/autowire :fx/entity} laptop
  [:spec {:table "laptop"}
   [:id :uuid]
   [:chip ::chip]
   [:user {:wrap true} :int]
   [:some-field :string]])


(deftest enum-type-test
  (let [config (duct/prep-config config)
        system (ig/init config)
        ds     (val (ig/find-derived-1 system :fx.database/connection))
        laptop (val (ig/find-derived-1 system ::laptop))
        lt-id  (random-uuid)]

    (is (fx.migrate/table-exist? ds "laptop"))

    (let [columns (mdl/index-by :column-name (get-columns ds "laptop"))]
      (is (= "laptop_chips" (get-in columns ["chip" :udt-name]))
          "enum column created correctly"))

    (fx.repo/save! laptop {:id         lt-id
                           :chip       "intel"
                           :user       1
                           :some-field "test"})

    (let [{:keys [chip]} (fx.repo/find! laptop {:id         lt-id
                                                :chip       "intel"
                                                :user       1
                                                :some-field "test"})]
      (is (= "intel" chip)
          "enum values are stored correctly"))

    (fx.repo/update! laptop
                     {:chip       "amd"
                      :some-field "another test"}
                     {:id lt-id})

    (let [{:keys [chip]} (fx.repo/find! laptop {:id lt-id})]
      (is (= "amd" chip)
          "enum values are updated correctly"))

    (ig/halt! system)))


(def ^{:fx/autowire :fx/entity} alarm
  [:spec {:table "alarm"}
   [:last-run :timestamp]
   [:last-run-local :timestamp-tz]
   [:day :date]
   [:time :time]
   [:time-local :time-tz]
   [:period-daily [:interval {:fields "DAY"}]]
   [:period :interval]])


(deftest time-type-test
  (let [config (duct/prep-config config)
        system (ig/init config)
        ds     (val (ig/find-derived-1 system :fx.database/connection))
        alarm  (val (ig/find-derived-1 system :fx.data-types-test/alarm))]

    (is (fx.migrate/table-exist? ds "alarm"))

    (let [columns (mdl/index-by :column-name (get-columns ds "alarm"))]
      (is (= "timestamp without time zone" (get-in columns ["last_run" :data-type])))
      (is (= "timestamp with time zone" (get-in columns ["last_run_local" :data-type])))
      (is (= "date" (get-in columns ["day" :data-type])))
      (is (= "time without time zone" (get-in columns ["time" :data-type])))
      (is (= "time with time zone" (get-in columns ["time_local" :data-type])))
      (is (= "interval" (get-in columns ["period_daily" :data-type])))
      (is (= "DAY" (get-in columns ["period_daily" :interval-type])))
      (is (= "interval" (get-in columns ["period" :data-type])))
      (is (= nil (get-in columns ["period" :interval-type]))))

    (fx.repo/save! alarm {:last-run       (LocalDateTime/of 2022 10 1 13 0)
                          :last-run-local (OffsetDateTime/of 2022 10 1 13 0 0 0 (ZoneOffset/ofHours 3))
                          :day            (LocalDate/of 2022 10 1)
                          :time           (LocalTime/of 13 30 0)
                          :time-local     (OffsetTime/of (LocalTime/of 13 30 0) (ZoneOffset/ofHours 3))
                          :period-daily   (Duration/ofDays 5)
                          :period         (Duration/ofMinutes 5)})

    (let [{:keys [last-run last-run-local day time time-local period-daily period]}
          (fx.repo/find! alarm {:day (LocalDate/of 2022 10 1)})]
      (is (.isEqual last-run (LocalDateTime/of 2022 10 1 13 0)))
      (is (.isEqual last-run-local (LocalDateTime/of 2022 10 1 12 0)))
      (is (.isEqual day (LocalDate/of 2022 10 1)))
      (is (.equals time (LocalTime/of 13 30 0)))
      (is (.equals period-daily (Duration/ofDays 5)))
      (is (.equals period (Duration/ofMinutes 5)))

      (is (.equals (Duration/ofMinutes 1) (Duration/ofSeconds 60))))

    (ig/halt! system)))


(def ^{:fx/autowire :fx/entity} order
  [:spec {:table "order"}
   [:id {:identity true} :uuid]
   [:price :int]
   [:items [:array {:of :string}]]])


(deftest array-type-test
  (with-system [system config]
    (let [ds       (val (ig/find-derived-1 system :fx.database/connection))
          order    (val (ig/find-derived-1 system :fx.data-types-test/order))
          order-id (random-uuid)]
      (is (fx.migrate/table-exist? ds "order"))

      (let [columns (mdl/index-by :column-name (get-columns ds "order"))]
        (is (= "ARRAY" (get-in columns ["items" :data-type]))))

      (fx.repo/save! order {:id    order-id
                            :price 245
                            :items ["Bag" "Milk" "Bread"]})

      (let [{:keys [items]} (fx.repo/find! order {:id order-id})]
        (is (sequential? items))
        (is (= 3 (count items)))))))


(def ^{:fx/autowire :fx/entity} fruit
  [:spec {:enum   "fruit"
          :values ["apple" "banana" "orange"]}])


(def fruit-extended
  [:spec {:enum   "fruit"
          :values ["apple" "banana" "orange" "cherry"]}])


(deftest array-type-test
  (with-system [system config]
    (let [ds (val (ig/find-derived-1 system :fx.database/connection))]
      (is (= ["apple" "banana" "orange"]
             (fx.migrate/get-db-enum-values ds "fruit")))

      (let [fruit        (-> fruit-extended entity/prepare-spec :spec)
            fruit-entity (entity/create-entity :fx.data-types-test/fruit fruit)]

        (fx.migrate/apply-migrations! {:database ds
                                       :entities #{fruit-entity}})

        (is (= ["apple" "banana" "orange" "cherry"]
               (fx.migrate/get-db-enum-values ds "fruit")))))))
