(ns emdr-client.persistence.rethink
  (:require [com.stuartsierra.component :as comp]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan close! thread <!! >!!
                                                  go <! >!]]
            [emdr-client.common.utils :refer [close-chan order? history?
                                              result-transducer]]
            [rethinkdb.query :as r]))



(defn- insert-table
  [conn db table data]
  (try
    (->
      (r/db db)
      (r/table table)
      (r/insert data)
      (r/run conn))
    (catch Exception e
      (log/error (str "Error occured writing payload to " table " with " data " Exception: " e))))

  (defn start-rethinkdb-writers
    "Start the specified number of threads, to write to RethinkDB"
    [conn db table channel n-consumers]
    (dotimes [_ n-consumers]
      (thread
        (loop []
          (if-let [c (<!! channel)]
            (if (seq c)
              (do
                (insert-table conn db table c)
                (recur))
              (recur))
            (log/info "Stopping consumer because Market Data connection lost.")))))))

(defrecord RethinkDB [rethink-chans]
  comp/Lifecycle
  (start [comp]
    (log/info "Starting RethinkDB Consumer")
    (let [conn (r/connect :host "127.0.0.1" :port 28015)]
      (start-rethinkdb-writers conn "emdr" "orders" (:order-chan rethink-chans) 5)
      (start-rethinkdb-writers conn "emdr" "history" (:history-chan rethink-chans) 5)
      (assoc comp :conn conn)))
  (stop [comp]
    (log/info "Shutting down RethinkDB Consumer")
    (->
      (update-in comp [:rethink-chans :order-chan] close-chan)
      (update-in [:rethink-chans :history-chan] close-chan))))

(defn new-rethinkDB-consumer []
  (map->RethinkDB {}))

(defrecord RethinkDBChannels [buffer-size]
  comp/Lifecycle
  (start [comp]
    (log/infof "Initializing RethinkDB channels with size %d" buffer-size)
    (->
      (assoc comp :order-chan (chan buffer-size (result-transducer order?)))
      (assoc :history-chan (chan buffer-size (result-transducer history?)))))
  (stop [comp]
    (log/info "Closing RethinkDB Channels")
    (->
      (update-in comp [:order-chan] close-chan)
      (update-in [:history-chan] close-chan))))

(defn new-rethinkdb-channels [buffer-size]
  (map->RethinkDBChannels {:buffer-size buffer-size}))