(ns fortress.commons-kafka.components.topic
  (:require [clojure.walk :as walk])
  (:import [org.apache.kafka.clients.admin AdminClient NewTopic]))

(defn ^:private admin [config]
  (AdminClient/create (walk/stringify-keys config)))

(defn ^:private ->NewTopic [{:keys [topic-name partitions replications]}]
  (NewTopic. topic-name partitions replications))

(defn create-topics [topics config]
  (let [admin (admin config)]
    (->> topics
         (map ->NewTopic)
         (.createTopics admin))))