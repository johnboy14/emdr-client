(ns emdr-client.common.utils
  (:require [clojure.core.async :refer [close!]])
  (:import (java.util UUID)))

(defn close-chan [chan]
  (when chan
    (close! chan)))

(defn order? [msg]
  (if (= (:resultType msg) "orders")
    true))

(defn history? [msg]
  (if (= (:resultType msg) "history")
    true))

(defn- zip-rowset [{:keys [columns rowsets resultType]}]
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