(ns fortress.commons-kafka.configs.specs
  (:require [clojure.spec.alpha :as s])
  (:import java.util.regex.Pattern))

(s/def ::bootstrap.servers string?)

(s/def ::auto-correlation-id? boolean?)

(s/def ::config-producer
  (s/keys :req-un [::bootstrap.servers]
          :opt-un [::auto-correlation-id?]))

(s/def ::group.id string?)

(s/def ::topics (s/coll-of string?))

(s/def ::pattern #(instance? Pattern %))

(s/def ::config-consumer
  (s/keys :req-un [::bootstrap.servers
                   ::group.id]))

(s/def ::handler (constantly true))
(s/def ::handler-exception (constantly true))
(s/def ::filter-message-retryable (constantly true))
(s/def ::auto-retry? boolean?)
(s/def ::consumer-dlq? boolean?)
(s/def ::topic-name string?)
(s/def ::partitions pos-int?)
(s/def ::replications pos-int?)

(s/def ::config-topic
  (s/keys :req-un [::topic-name
                   ::partitions
                   ::replications]))

(s/def ::config-run
  (s/keys :req-un [::handler
                   ::auto-retry?
                   ::consumer-dlq?]
          :opt-un [::handler-exception]))

(s/def ::consumer
  (s/keys :req-un [::config-consumer
                   ::config-run]
          :opt-un [::topics
                   ::pattern]))

(s/def ::retries pos-int?)
(s/def ::delay-seg pos-int?)

(s/def ::retry
  (s/keys :req-un [::config-topic
                   ::retries
                   ::delay-seg]
          :opt-un [::config-producer
                   ::config-consumer
                   ::filter-message-retryable
                   ::config-run]))

(s/def ::dlq
  (s/keys :req-un [::config-topic]
          :opt-un [::config-producer
                   ::config-consumer
                   ::config-run]))

(s/def ::consumer-component
  (s/keys :req-un [::consumer]
          :opt-un [::retry
                   ::dlq]))