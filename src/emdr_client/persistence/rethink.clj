(ns emdr-client.persistence.rethink
  (:require [com.stuartsierra.component :as comp]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [chan close! thread <!! >!!
                                                  go <! >!]]
            [emdr-client.common.utils :refer [close-chan]]
            [rethinkdb.query :as r])
  (:import (java.util UUID)))

(defn- order? [msg]
  (if (= (:resultType msg) "orders")
    true))

(defn- history? [msg]
  (if (= (:resultType msg) "history")
    true))

(defn zip-rowset [{:keys [columns rowsets resultType]}]
  (loop [rowset rowsets
         acc []]
    (if (seq rowset)
      (->>
        (map #(->
               (zipmap columns %)
               (assoc :resultType resultType :id (.toString (UUID/randomUUID)))
               (merge (select-keys (first rowset) [:regionID :typeID]))
               clojure.walk/keywordize-keys)
         (:rows (first rowset)))
        (conj acc)
        (recur (rest rowset)))
      (flatten acc))))

(def result-transducer
  (fn [pred]
    (comp
      (filter pred)
      (map zip-rowset))))

(defn- insert-table
  [conn db table data timeout]
  (-> (r/db db)
    (r/table table)
    (r/insert data)
    (r/run conn timeout)))

(defrecord RethinkDB [rethink-chans]
  comp/Lifecycle
  (start [comp]
    (log/info "Starting RethinkDB Consumer")
    (let [conn (r/connect :host "127.0.0.1" :port 28015)]
      (thread
        (loop []
          (if-let [c (<!! (:order-chan rethink-chans))]
            (if (seq c)
              (do
                (insert-table conn "emdr" "orders" c 2)
                (recur))
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
      (assoc comp :order-chan (chan buffer-size (result-transducer order?)))
      (assoc :history-chan (chan buffer-size (result-transducer history?)))))
  (stop [comp]
    (log/info "Closing RethinkDB Channels")
    (->
      (update-in comp [:order-chan] close-chan)
      (update-in [:history-chan] close-chan))))

(defn new-rethinkdb-channels [buffer-size]
  (map->RethinkDBChannels {:buffer-size buffer-size}))