(ns fx.module.repo-test
  (:require
   [clojure.test :refer :all]
   [duct.core :as duct]
   [integrant.core :as ig]
   [malli.instrument :as mi]
   [fx.repo]))


(duct/load-hierarchy)
(mi/instrument!)


(def ^{:fx/autowire :fx/entity} person
  [:spec {:table "person"}
   [:id {:identity true} :uuid]
   [:name :string]
   [:column {:wrap true} :string]])


(def config
  {:duct.profile/base                 {:duct.core/project-ns 'test}
   :fx.module/autowire                {:root 'fx.module.repo-test}
   :fx.module/repo                    {:migrate {:strategy :update-drop}}
   :fx.containers.postgres/connection {}})


(deftest repo-module-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (testing "repo module initialized all sub components"
      (is (not-empty (ig/find-derived-1 system :fx.repo/migrate)))
      (is (not-empty (ig/find-derived-1 system :fx.repo/adapter))))

    (testing "entity extended by repo module"
      (let [person    (val (ig/find-derived-1 system ::person))
            person-id (random-uuid)]
        (fx.repo/save! person {:id     person-id
                               :column "base"
                               :name   "Mighty Repo"})

        (is (= (fx.repo/find! person {:id person-id})
               {:id     person-id
                :column "base"
                :name   "Mighty Repo"}))))

    (ig/halt! system)))


(deftest save-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (let [person (val (ig/find-derived-1 system ::person))]
      (testing "save single entity record"
        (fx.repo/save! person {:id     (random-uuid)
                               :column "base"
                               :name   "user-1"})

        (is (= #{"user-1"}
               (->> (fx.repo/find-all! person)
                    (map :name)
                    set))))

      (testing "save multiple entity records in one go"
        (fx.repo/save-all! person [{:id     (random-uuid)
                                    :column "base"
                                    :name   "user-2"}
                                   {:id     (random-uuid)
                                    :column "base"
                                    :name   "user-3"}])

        (is (= #{"user-1" "user-2" "user-3"}
               (->> (fx.repo/find-all! person)
                    (map :name)
                    set)))))

    (ig/halt! system)))


(deftest find-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (let [person    (val (ig/find-derived-1 system ::person))
          person-id (random-uuid)]
      (fx.repo/save! person {:id     person-id
                             :column "base"
                             :name   "Mighty Repo"})

      (testing "simple key value match"
        (is (= person-id
               (:id (fx.repo/find! person {:id person-id}))))

        (is (= person-id
               (:id (fx.repo/find! person {:name "Mighty Repo"}))))

        (is (= person-id
               (:id (fx.repo/find! person {:column "base"})))))

      (testing "using HoneySQL operators"
        (is (= person-id
               (:id (fx.repo/find! person {:fx.repo/where [:in :id [person-id]]}))))

        (is (= person-id
               (:id (fx.repo/find! person {:column "base"
                                           :fx.repo/where  [:or [:= :name "random name"]
                                                            [:= :id person-id]]}))))

        (is (= person-id
               (:id (fx.repo/find! person {:fx.repo/where [:= [:raw "\"column\""] "base"]})))))

      (testing "using :quote function to wrap SQL reserved keywords"
        (is (= person-id
               (:id (fx.repo/find! person {:fx.repo/where [:= [:quote :column] "base"]})))))

      (testing "specifying the returning columns"
        (let [result (fx.repo/find! person {:id     person-id
                                            :fx.repo/fields [:name]})]
          (is (= 1 (count result)))
          (is (= {:name "Mighty Repo"} result)))))

    (ig/halt! system)))


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:identity true} :uuid]
   [:name :string]
   [:post {:rel-type :one-to-one} ::post]])


(def ^{:fx/autowire :fx/entity} post
  [:spec {:table "post"}
   [:id {:identity true} :uuid]
   [:title :string]
   [:users {:rel-type :one-to-many} ::user]])


(deftest find-with-nested-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (let [user    (val (ig/find-derived-1 system ::user))
          user-id (random-uuid)
          post    (val (ig/find-derived-1 system ::post))
          post-id (random-uuid)]

      (fx.repo/save! post {:id    post-id
                           :title "Random post"})
      (fx.repo/save! user {:id   user-id
                           :name "Random user"
                           :post post-id})

      (testing "get user with related post"
        (let [result (fx.repo/find! user {:id     user-id
                                          :fx.repo/nested true})]
          (is (= user-id (:id result)))
          (is (map? (:post result)))
          (is (= post-id (get-in result [:post :id]))))))

    (ig/halt! system)))


(deftest find-all-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (let [user     (val (ig/find-derived-1 system ::user))
          user-id  (random-uuid)
          user2-id (random-uuid)
          post     (val (ig/find-derived-1 system ::post))
          post-id  (random-uuid)]

      (fx.repo/save! post {:id    post-id
                           :title "Random post"})
      (fx.repo/save! user {:id   user-id
                           :name "Random user"
                           :post post-id})

      (testing "get all users"
        (let [result (fx.repo/find-all! user)]
          (is (= 1 (count result))))

        (fx.repo/save! user {:id   user2-id
                             :name "Second user"
                             :post post-id})

        (let [result (fx.repo/find-all! user {:fx.repo/nested true})]
          (is (= 2 (count result)))))

      (testing "get all posts"
        (let [result (fx.repo/find-all! post)]
          (is (= 1 (count result))))

        (let [result     (fx.repo/find-all! post {:fx.repo/nested true})
              first-post (first result)]
          (is (= 2 (count (:users first-post))))
          (is (= #{user-id user2-id} (set (map :id (:users first-post))))))))

    (ig/halt! system)))


(deftest update-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (let [post    (val (ig/find-derived-1 system ::post))
          post-id (random-uuid)]

      (fx.repo/save! post {:id    post-id
                           :title "New post"})

      (let [result (fx.repo/find! post {:id post-id})]
        (is (= "New post" (:title result))))

      (let [result (fx.repo/update! post {:title "Already old post"} {:id post-id})]
        (is (= "Already old post" (:title result))))

      (let [result (fx.repo/find! post {:id post-id})]
        (is (= "Already old post" (:title result)))))

    (ig/halt! system)))


(deftest delete-test
  (let [config (duct/prep-config config)
        system (ig/init config)]

    (let [post    (val (ig/find-derived-1 system ::post))
          post-id (random-uuid)]

      (fx.repo/save! post {:id    post-id
                           :title "New post"})

      (let [result (fx.repo/find! post {:id post-id})]
        (is (= "New post" (:title result))))

      (let [result (fx.repo/delete! post {:id post-id})]
        (is (= "New post" (:title result))))

      (let [result (fx.repo/find! post {:id post-id})]
        (is (nil? result))))

    (ig/halt! system)))
