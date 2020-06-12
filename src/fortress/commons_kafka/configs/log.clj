(ns fortress.commons-kafka.configs.log
  (:require [clojure.pprint]
            [tick.core :as t]))

(defn ^:private now []
  (str (t/now)))

(defn ^:private message-format 
  ([level message data]
   (str "LOG "
        "[" level "] "
        "Message: "
        message " "
        "Data: "
        data))
  ([level message data ex]
   (str (message-format level message data) " "
        "Exception: "
        (.getMessage ex))))

(defn info [message data]
  (prn (message-format "INFO" message data)))

(defn warn [message data]
  (prn (message-format "WARN" message data)))

(defn error [message data exception]
  (prn (message-format "ERROR" message data exception)))