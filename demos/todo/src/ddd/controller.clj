(ns ddd.controller
  (require [ddd.service :as user-service]
           [fx.repo :as fx.repo]))


(defn ^:fx/autowire create-user [^:ddd.service/user user user-data]
  (fx.repo/save! user user-data)

  (fx.repo/find! user {:id "some-id"})

  (fx.repo/find! user {:where    [:and
                                  [:user/name ""]
                                  [:user/last-name ""]]
                       :limit    10
                       :order-by [:user/name :desc]}))
