(ns fortress.commons-kafka.configs.serdes
  (:import org.apache.kafka.common.serialization.Serdes))

(defn ^:private string-serde []
  (Serdes/String))

(defn ^:private class-name->string [c]
  (.getName (class c)))

(defn string-serializer
  "Return Serializer from string"
  []
  {:key.serializer (class-name->string (.serializer (string-serde)))
   :value.serializer (class-name->string (.serializer (string-serde)))})