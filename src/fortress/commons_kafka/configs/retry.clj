(ns fortress.commons-kafka.configs.retry
  (:require [fortress.commons-kafka.components.producer :as f-producer]
            [fortress.commons-kafka.components.topic :as f-topic]
            [fortress.commons-kafka.configs.specs :as f-specs]
            [fortress.commons-kafka.configs.data :as f-data]
            [clojure.spec.alpha :as s]))

(defn ^:private retry-or-dlq
  [{:keys [filter-message-retryable] :as retry} dlq record exception]
  (if (filter-message-retryable record exception)
    retry
    dlq))

(defn ^:private send-topic-retry-dlq
  [value key {:keys [config-producer config-topic]}]
  (clojure.pprint/pprint value)
  (f-producer/send! config-producer
                    (:topic-name config-topic)
                    key
                    (f-data/map->string-json value)))

(defn handler-exception-auto-retry
  [{:keys [handler-exception]
    :or {handler-exception (constantly nil)}}
   {:keys [retry dlq]}]
  (fn [record e]
    (-> record
        f-data/consumer-record->map
        f-data/encapsulated-record
        (send-topic-retry-dlq (.key record) (retry-or-dlq retry dlq record e)))
    (handler-exception record e)))

(defn ^:private consumer-handler-retry [{:keys [config-run]} {:keys [delay-seg]}]
  (let [handler (:handler config-run)]
    (fn [record]
      (prn "entrou retry" delay-seg)
      (Thread/sleep (* delay-seg 1000))
      (-> (.value record)
          f-data/string-json->map
          :record
          (assoc :timestamp (.timestamp record))
          f-data/map->consumer-record
          handler))))

(defn update-new-attempt [record timestamp]
  (-> record
      (update :attempts conj timestamp)
      (update :retried inc)))

(defn ^:private consumer-handler-exception-retry [{:keys [retry dlq]}]
  (let [{:keys [retries]} retry]
    (fn [record _]
      (let [{:keys [timestamp value key]} (f-data/consumer-record->map record)
            {:keys [retried max-retries] 
             :or {max-retries retries}
             :as new-record} (update-new-attempt (f-data/string-json->map value) timestamp)]
        (if (< retried max-retries)
          (send-topic-retry-dlq new-record key retry)
          (send-topic-retry-dlq new-record key dlq))))))

(defn ^:private merge-consumer-with-retry-dlq
  [{:keys [config-consumer config-run]} retry-dlq]
  (let [new-config-run (dissoc config-run :handler)]
    (-> retry-dlq
        (update :config-consumer #(merge config-consumer %))
        (update :config-run #(merge new-config-run %))
        (update :config-producer #(merge (select-keys config-consumer [:bootstrap.servers]) %)))))

(defn init-component-retry-dlq
  [{:keys [consumer retry dlq] :as config-component}]
  {:pre [(and (s/valid? ::f-specs/retry retry)
              (s/valid? ::f-specs/dlq dlq))]}
  (f-topic/create-topics [(:config-topic retry)
                          (:config-topic dlq)]
                         (select-keys (:config-consumer consumer) [:bootstrap.servers]))
  (let [new-config-component (-> config-component
                                 (update :retry #(merge-consumer-with-retry-dlq consumer %))
                                 (update :dlq #(merge-consumer-with-retry-dlq consumer %)))]
    (-> new-config-component
        (assoc-in [:retry :config-run :handler] (consumer-handler-retry consumer retry))
        (assoc-in [:retry :config-run :handler-exception] (consumer-handler-exception-retry new-config-component)))))