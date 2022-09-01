(ns fx.repo)


(defprotocol IRepo
  (save! [entity data])
  (update! [entity data options])
  (delete! [entity options])
  (find! [entity options]))
