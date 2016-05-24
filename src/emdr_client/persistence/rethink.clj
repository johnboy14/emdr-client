(ns emdr-client.persistence.rethink
  (:require [com.stuartsierra.component :as comp]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan close!]]
            [emdr-client.common.utils :refer [close-chan]]))

(defrecord RethinkDBChannels [buffer-size]
  comp/Lifecycle
  (start [comp]
    (log/infof "Initializing RethinkDB channels with size %d" buffer-size)
    (->
      (assoc comp :order-chan (chan buffer-size (filter #(= "orders" (:resultType %)))))
      (assoc :history-chan (chan buffer-size (filter #(= "history" (:resultType %)))))))
  (stop [comp]
    (log/info "Closing RethinkDB Channels")
    (->
      (update-in comp [:order-chan] close-chan)
      (update-in [:history-chan] close-chan))))

(defn new-rethinkdb-channels [buffer-size]
  (map->RethinkDBChannels {:buffer-size buffer-size}))