(ns emdr-client.persistence.rethink
  (:require [com.stuartsierra.component :as comp]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan close! thread <!! >!!]]
            [emdr-client.common.utils :refer [close-chan order? history?
                                              result-transducer]]
            [rethinkdb.query :as r]
            [rethinkdb.core :as rc]))

(defn- initialize-tables [conn db tables]
  (let [existing-tables (->
                          (r/db db)
                          (r/table-list)
                          (r/run conn)
                          set)]
    (doseq [t tables]
      (when-not (existing-tables t)
        (do
          (log/info "Creating Tables " tables " in " db)
          (-> (r/db db) (r/table-create t) (r/run conn) r/wait))))))

(defn- initialize-db [conn db tables]
  (let [d ((-> (r/db-list) (r/run conn) set) db)]
    (when-not d
      (log/info "Create Database " db)
      (-> (r/db-create db) (r/run conn) r/wait))
    (initialize-tables conn db tables)))

(defn- close-rethink-conn [conn]
  (when-not (= :closed conn)
    (rc/close conn)))

(defn- insert-table
  [conn db table data]
  (when (seq data)
    (log/infof "Writing %d records to table %s" (count data) table)
    (try
      (->
        (r/db db)
        (r/table table)
        (r/insert data)
        (r/run conn))
      (catch Exception e
        (log/error (str "Error occured writing payload to " table " with " data " Exception: " e))))))

(defn- start-rethinkdb-writers
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
          (log/info "Stopping consumer because Market Data connection lost."))))))

(defrecord RethinkDBWriter [host port n-writers rethink-chans]
  comp/Lifecycle
  (start [comp]
    (log/info "Starting RethinkDB Consumer")
    (let [conn (r/connect :host host :port port)]
      (initialize-db conn "emdr" ["orders" "history"])
      (start-rethinkdb-writers conn "emdr" "orders" (:order-chan rethink-chans) n-writers)
      (start-rethinkdb-writers conn "emdr" "history" (:history-chan rethink-chans) n-writers)
      (assoc comp :conn conn)))
  (stop [comp]
    (log/info "Shutting down RethinkDB Consumer")
    (->
      (update-in comp [:conn] close-rethink-conn)
      (update-in [:rethink-chans :order-chan] close-chan)
      (update-in [:rethink-chans :history-chan] close-chan))))

(defn new-rethinkDB-writers [host port n-writers]
  (map->RethinkDBWriter {:host host :port port :n-writers n-writers}))

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