(ns fortress.commons-kafka.configs.data
  (:require [cheshire.core :as json])
  (:import org.apache.kafka.clients.consumer.ConsumerRecord
           [org.apache.kafka.common.header.internals RecordHeader RecordHeaders]))

(defn map->string-json [m]
  (json/generate-string m))

(defn string-json->map [s]
  (json/parse-string s true))

(defn encapsulated-record
  [record]
  {:attempts [(:timestamp record)]
   :retried 0
   :record record})

(defn headers->map [headers]
  (->> (.toArray headers)
       (map (fn [h] {(keyword (.key h)) (.value h)}))))

(defn consumer-record->map
  [record]
  {:key (.key record)
   :value (.value record)
   :topic (.topic record)
   :headers (headers->map (.headers record))
   :timestamp (.timestamp record)
   :timestampType (.timestampType record)
   :partition (.partition record)
   :offset (.offset record)
   :checksum (.checksum record)
   :serializedKeySize (.serializedKeySize record)
   :serializedValueSize (.serializedValueSize record)})

(defn map->headers [headers [k v]]
  (->> (RecordHeader. (name k) (.getBytes v))
       (.add headers)))

(defn map->consumer-record
  [{:keys [key value topic headers timestamp timestampType partition offset checksum serializedKeySize serializedValueSize]}]
  (let [headers (reduce headers->map (RecordHeaders.) headers)]
    (ConsumerRecord. topic partition offset timestamp timestampType checksum serializedKeySize serializedValueSize key value headers)))