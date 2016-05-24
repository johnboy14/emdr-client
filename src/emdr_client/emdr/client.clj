(ns emdr-client.emdr.client
  (:require [com.stuartsierra.component :as comp]
            [clojure.core.async :refer [chan close! thread >!! <!!]]
            [clojure.tools.logging :as log]
            [cheshire.core :refer [parse-string]])
  (:import (org.zeromq ZMQ)
           (java.util.zip Inflater)))

(defn- close-chan [chan]
  (when chan
    (close! chan)))

(defn- inflater
  "Decompresses data from Relay and returns JSON payload as Clojure Map."
  [data]
  (when data
    (let [inflater (Inflater.)
          decompressed (byte-array (* (alength data) 16))
          _ (.setInput inflater data)
          decompressed-size (.inflate inflater decompressed)
          output (byte-array decompressed-size)]
      (System/arraycopy decompressed 0 output 0 decompressed-size)
      (->
        (String. output "UTF-8")
        (parse-string true)))))

(defn- market-data
  "Subscribes to specified EMDR Relay, returning channel with new market data."
  [relay buffer-size]
  (let [out (chan buffer-size)
        context (ZMQ/context 1)
        subscriber (.socket context ZMQ/SUB)]
    (.connect subscriber relay)
    (.subscribe subscriber (byte-array 0))
    (thread
      (try
        (loop []
          (let [data (.recv subscriber)]
            (when true
              (do
                (>!! out data)
                (recur)))))
        (catch Throwable ex
          (log/errorf "Error occured processing EMDR with '%s'" (.getMessage ex))))
      (.term context))
    out))

(defn start-emdr-consumers
  "Start the specified number of threads, to consume the EMDR relay and place
  order data on order-chan and history data on history-chan"
  [market-data-chan mkdata-chan n-consumers]
  (dotimes [_ n-consumers]
    (thread
      (loop []
        (if-let [c (inflater (<!! market-data-chan))]
          (do
            (log/infof "ResultType = %s" (:resultType c))
            (>!! mkdata-chan c)
            (recur))
          (log/info "Stopping consumer because Market Data connection lost."))))))

(defrecord EMDRClient [relay n-consumers mkdata-chan]
  comp/Lifecycle
  (start [comp]
    (log/infof "Starting EMDR Client with %d consumers of %s" n-consumers relay)
    (let [m-chan (market-data relay 10)]
      (start-emdr-consumers m-chan (:chan mkdata-chan) n-consumers)
      (assoc comp :market-data-chan m-chan)))
  (stop [comp]
    (log/infof "Shutting down EMDR Client")
    (->
      (update-in comp [:market-data-chan] close-chan)
      (update-in [:mkdata-chan :chan] close-chan))))

(defn new-emdr-client [relay n-consumers]
  (map->EMDRClient {:relay relay :n-consumers n-consumers}))

(defrecord Channel [buffer-size]
  comp/Lifecycle
  (start [comp]
    (log/infof "Creating channel with buffer-size %d" buffer-size)
    (assoc comp :chan (chan buffer-size)))
  (stop [comp]
    (log/info "Closing Channel")
    (update-in comp [:chan] close-chan)))

(defn new-channel [buffer-size]
  (map->Channel {:buffer-size buffer-size}))


