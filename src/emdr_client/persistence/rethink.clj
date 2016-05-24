(ns emdr-client.persistence.rethink
  (:require [com.stuartsierra.component :as comp]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan close! thread <!!]]
            [emdr-client.common.utils :refer [close-chan]]
            [rethinkdb.query :as r]))

(defn- order? [msg]
  (if (= (:resultType msg) "orders")
    true))

(defn- history? [msg]
  (if (= (:resultType msg) "history")
    true))

(defrecord RethinkDB [rethink-chans]
  comp/Lifecycle
  (start [comp]
    (log/info "Starting RethinkDB Consumer")
    (let [conn (r/connect :host "127.0.0.1" :port 28015 :db "emdr")]
      (thread
        (loop []
          (if-let [c (<!! (:order-chan rethink-chans))]
            (do
              (r/run (r/insert "orders" [c]) conn)
              (recur))
            (log/info "Stopping consumer because Market Data connection lost."))))
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
      (assoc comp :order-chan (chan buffer-size (filter order?)))
      (assoc :history-chan (chan buffer-size (filter history?)))))
  (stop [comp]
    (log/info "Closing RethinkDB Channels")
    (->
      (update-in comp [:order-chan] close-chan)
      (update-in [:history-chan] close-chan))))

(defn new-rethinkdb-channels [buffer-size]
  (map->RethinkDBChannels {:buffer-size buffer-size}))