(ns fortress.commons-kafka.configs.serdes
  (:import org.apache.kafka.common.serialization.Serdes))

(defn ^:private string-serde []
  (Serdes/String))

(defn ^:private class-name->string [c]
  (.getName (class c)))

(def ^:private string-serializer
  (class-name->string (.serializer (string-serde))))

(def ^:private string-deserializer
  (class-name->string (.deserializer (string-serde))))

(def config-string-serializer
  "Return Serializer from string"
  {:key.serializer string-serializer
   :value.serializer string-serializer})

(def config-string-deserializer
  "Return Deserializer from string"
  {:key.deserializer string-deserializer
   :value.deserializer string-deserializer})