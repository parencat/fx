(ns fx.constraints-test
  (:require
   [clojure.test :refer :all]
   [fx.system :refer [with-system]]
   [fx.migrate]
   [fx.repo]
   [duct.core :as duct]
   [malli.instrument :as mi]
   [integrant.core :as ig]
   [fx.entity :as entity]))


(duct/load-hierarchy)
(mi/instrument!)


(def config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.constraints-test}
   :fx.module/repo                    {:migrate {:strategy :update-drop}}
   :fx.containers.postgres/connection {}})


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:identity true} :uuid]
   [:first-name {:default "Uncle Bob"} :string]])


(def mod-user-spec
  [:spec {:table "user"}
   [:id {:identity true} :uuid]
   [:first-name {:default "Jack Sparrow"} :string]])


(def mod-user-no-default-spec
  [:spec {:table "user"}
   [:id {:identity true} :uuid]
   [:first-name :string]])


(deftest default-value-test
  (with-system [system config]
    (let [ds   (val (ig/find-derived-1 system :fx.database/connection))
          user (val (ig/find-derived-1 system :fx.constraints-test/user))
          leo  (random-uuid)
          bob  (random-uuid)]
      (is (fx.migrate/table-exist? ds "user"))

      (testing "saving with default value"
        (fx.repo/save! user {:id         leo
                             :first-name "Leo"})

        (let [{:keys [first-name]} (fx.repo/find! user {:id leo})]
          (is (= "Leo" first-name)))

        (fx.repo/save! user {:id bob})

        (let [{:keys [first-name]} (fx.repo/find! user {:id bob})]
          (is (= "Uncle Bob" first-name))))

      (testing "altering default value"
        (let [user-spec   (-> mod-user-spec entity/prepare-spec :spec)
              user-entity (entity/create-entity :fx.constraints-test/user user-spec)
              jack        (random-uuid)]

          (fx.migrate/apply-migrations! {:database ds
                                         :entities #{user-entity}})

          (fx.repo/save! user {:id jack})

          (let [{:keys [first-name]} (fx.repo/find! user {:id jack})]
            (is (= "Jack Sparrow" first-name)))))

      (testing "dropping default value"
        (let [user-spec   (-> mod-user-no-default-spec entity/prepare-spec :spec)
              user-entity (entity/create-entity :fx.constraints-test/user user-spec)
              random      (random-uuid)]

          (fx.migrate/apply-migrations! {:database ds
                                         :entities #{user-entity}})

          (is (thrown? Exception
                       (fx.repo/save! user {:id random})))

          (fx.repo/save! user {:id         random
                               :first-name "Random"})

          (let [{:keys [first-name]} (fx.repo/find! user {:id random})]
            (is (= "Random" first-name))))))))


(def ^{:fx/autowire :fx/entity} schedule
  [:spec {:table "schedule"}
   [:name :string]
   [:cron {:unique true} :string]])


(def non-unique-schedule
  [:spec {:table "schedule"}
   [:name :string]
   [:cron :string]])


(deftest unique-value-test
  (with-system [system config]
    (let [ds       (val (ig/find-derived-1 system :fx.database/connection))
          schedule (val (ig/find-derived-1 system :fx.constraints-test/schedule))]
      (is (fx.migrate/table-exist? ds "schedule"))

      (testing "can't save records with same values"
        (fx.repo/save! schedule {:name "somewhere in December"
                                 :cron "5 4 25 12 3"})

        (is (thrown? Exception
                     (fx.repo/save! schedule {:name "not this time"
                                              :cron "5 4 25 12 3"}))))

      (testing "altering unique value"
        (let [schedule-spec   (-> non-unique-schedule entity/prepare-spec :spec)
              schedule-entity (entity/create-entity :fx.constraints-test/schedule schedule-spec)]

          (fx.migrate/apply-migrations! {:database ds
                                         :entities #{schedule-entity}})

          (fx.repo/save! schedule {:name "every midnight"
                                   :cron "0 0 * * *"})

          (fx.repo/save! schedule {:name "every midnight"
                                   :cron "0 0 * * *"})

          (let [result (fx.repo/find-all! schedule {:cron "0 0 * * *"})]
            (is (= 2 (count result)))))))))



