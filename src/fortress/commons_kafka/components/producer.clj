(ns fortress.commons-kafka.components.producer
  (:require [clojure.walk :as walk]
            [clojure.spec.alpha :as s]
            [fortress.commons-kafka.configs.serdes :as f-serdes]
            [fortress.commons-kafka.configs.specs :as f-specs])
  (:import [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]))

(defn create-producer
  "Return KafkaProducer.
   It's mandatory to inform the keys in the config:
    - :bootstrap-servers => String
    - :key.serializer => String (default: org.apache.kafka.common.serialization.StringSerializer)
    - :value.serializer => String (default: org.apache.kafka.common.serialization.StringSerializer)"
  [config]
  {:pre [(s/valid? ::f-specs/config-producer config)]}
  (KafkaProducer. (walk/stringify-keys (merge f-serdes/config-string-serializer
                                              config))))

(defn ^:private add-headers-in-record!
  "Add headers map in record"
  [record headers]
  (let [record-headers (.headers record)]
    (doseq [[k v] headers]
      (.add record-headers
            (name k)
            (into-array Byte/TYPE (map byte v))))))

(defn ->record
  "Generate ProduceRecord from the fields:
     - topic-name 
     - key 
     - value 
     - healders (optional)"
  [topic-name key value headers]
  (let [record (ProducerRecord. topic-name key value)]
    (when headers
      (add-headers-in-record! record headers))
    record))

(defn send-message!
  "Send message to topic"
  ([producer record]
   (.send producer record))
  ([producer record callback]
   (.send producer record callback)))

(defn send!
  "Creates Producer, ProduceRecord and sends message"
  ([config-producer topic-name key value]
   (send! config-producer topic-name key value {}))
  ([config-producer topic-name key value headers]
   (let [producer (create-producer config-producer)
         record (->record topic-name key value headers)]
     (send-message! producer record))))