(ns fortress.commons-kafka.components.consumer
  (:require [clojure.walk :as walk]
            [clojure.spec.alpha :as s]
            [fortress.commons-kafka.configs.serdes :as f-serdes]
            [fortress.commons-kafka.configs.specs :as f-specs]
            [fortress.commons-kafka.configs.retry :as f-retry]
            [fortress.commons-kafka.configs.log :as log])
  (:import [org.apache.kafka.clients.consumer
            KafkaConsumer OffsetAndMetadata]
           [org.apache.kafka.common TopicPartition]
           java.time.Duration
           java.util.UUID))

(defn config-consumer-defaults []
  (merge f-serdes/config-string-deserializer
         {:client.id (str "consumer-" (UUID/randomUUID))}))

(defn create-consumer
  "Return KafkaConsumer.
   It's mandatory to inform the keys in the config:
     - :bootstrap-servers => String
     - :group.id => String
     - :key.serializer => String (default: org.apache.kafka.common.serialization.StringDeserializer)
     - :value.serializer => String (default: org.apache.kafka.common.serialization.StringDeserializer)
   It's also mandatory to inform one of the keys below:
     - :topics => [String] 
     - :pattern => java.util.regex.Pattern (case inform the two keys, it will be used)
   If the client.id field not informed, it will be generated automatically a uuid"
  [config]
  {:pre [(s/valid? ::f-specs/config-consumer config)]}
  (let [config-consumer (dissoc config :topics :pattern)
        config-with-defaults (merge (config-consumer-defaults)
                                    config-consumer)
        config-string (walk/stringify-keys config-with-defaults)]
    {:instance (KafkaConsumer. config-string)
     :client-id (:client.id config-with-defaults)}))

(defn topics-subscribe [instance topics pattern]
  (if topics
    topics
    (->> (.listTopics instance)
         keys
         (keep #(re-find pattern %)))))

(defn subscribe-topics
  "It's mandatory to inform the fields below:
     - consumer => KafkaConsumer
     - config => Map with one of the keys below:
                  > :topics => [String] (case inform the two keys, it will be used)
                  > :pattern => java.util.regex.Pattern"
  [{:keys [instance client-id]} {:keys [topics pattern] :as config}]
  {:pre [(s/valid? ::f-specs/consumer config)]}
  (if-let [list (topics-subscribe instance topics pattern)]
    (do
      (.subscribe instance list)
      (log/info "Consumer subscribing to topics" {:client-id client-id
                                                  :topics-param (first (keep identity [topics pattern]))
                                                  :topics-subscribed (.subscription instance)}))
    (throw (ex-info "It's mandatory to inform one of the keys [:topics or :pattern]" {:config config}))))

(defn consumer-init [consumer-info]
  (let [*running? (atom true)
        stoped (promise)]
    (merge consumer-info
           {:*running? *running?
            :stop #(swap! *running? (fn [_]
                                      (log/info "Stoping consumer-run!" consumer-info)
                                      (deliver stoped true)
                                      false))
            :stoped stoped
            :*count (atom 0)
            :processed (atom false)
            :*messages (atom clojure.lang.PersistentQueue/EMPTY)
            :*processed-messages (atom {})
            :*commit-messages (atom clojure.lang.PersistentQueue/EMPTY)
            :*pause? (atom false)})))

(defn ^:private next-message!
  [*queue]
  (let [message (peek @*queue)]
    (when message
      (swap! *queue pop))
    message))

(defn ^:private next-processing-message!
  [{:keys [*messages *count]}]
  (when-let [message (next-message! *messages)]
    (swap! *count inc)
    message))

(defn next-commit-messages! [processed-messages *commit-messages]
  (reset! *commit-messages processed-messages)
  {})

(defn commit-processed-messages!
  [consumer {:keys [*processed-messages *commit-messages] :as consumer-status}]
  (try
    (when (not-empty @*processed-messages)
      (swap! *processed-messages next-commit-messages! *commit-messages)
      (.commitSync consumer @*commit-messages (Duration/ofMillis 1000))
      (reset! *commit-messages clojure.lang.PersistentQueue/EMPTY))
    (catch Exception e
      (throw (ex-info "Error function commit-processed-messages!" {:fn-processing :commit!
                                                                   :consumer-status consumer-status
                                                                   :exception e})))))

(defn add-processed-messages! [{:keys [*processed-messages]} record]
  (let [topic-partition (TopicPartition. (.topic record) (.partition record))
        offset (OffsetAndMetadata. (inc (.offset record)))]
    (swap! *processed-messages assoc topic-partition offset)))

(defn ^:private consumer-handler
  [{:keys [handler handler-exception]} record]
  (try
    (handler record)
    (catch Exception e
      (log/error "Error processing message" {:record record} e)
      (try
        (when handler-exception
          (handler-exception record e))
        (catch Exception ex
          (log/error "Error processing handler-exception" {:record record} ex))))))

(defn ^:private handler-run!
  [config-run {:keys [*running? processed stop] :as consumer-status}]
  (try
    (while @*running?
      (reset! processed (promise))
      (when-let [record (next-processing-message! consumer-status)]
        (consumer-handler config-run record)
        (add-processed-messages! consumer-status record))
      (deliver @processed true))
    (catch Exception e
      (log/error "Error function handler-run!" {:config-run config-run
                                                :consumer-status consumer-status} e)
      (stop))))

(defn resume-consumer
  [consumer {:keys [*pause?]}]
  (.resume consumer (.assignment consumer))
  (swap! *pause? (constantly false)))

(defn pause-consumer
  [consumer {:keys [*pause?]}]
  (.pause consumer (.assignment consumer))
  (swap! *pause? (constantly true)))

(defn message-limit-exceeded?
  [{:keys [*messages]}]
  (> (count @*messages) 1))

(defn ^:private add-queue [messages records]
  (reduce (fn [acc rec] (conj acc rec)) messages records))

(defn add-records!
  [records {:keys [*messages]}]
  (swap! *messages #(add-queue % records)))

(defn ^:private poll! [consumer consumer-status]
  (try
    (let [{:keys [*pause?]} consumer-status
          exceeded? (message-limit-exceeded? consumer-status)]
      (if (not @*pause?)
        (when exceeded?
          (pause-consumer consumer consumer-status))
        (when (not exceeded?)
          (resume-consumer consumer consumer-status)))
      (let [records (.poll consumer (Duration/ofMillis 1000))]
        (add-records! records consumer-status)))
    (catch Exception e
      (throw (ex-info "Error function poll!" {:fn-processing :poll!
                                              :consumer-status consumer-status
                                              :exception e})))))

(defn ^:private consumer-processing
  [consumer {:keys [*running? processed] :as consumer-status}]
  (try
    (while @*running?
      (poll! consumer consumer-status)
      (commit-processed-messages! consumer consumer-status))
    (catch Exception e
      ((:stop consumer))
      (when @processed
        @@processed)
      (let [{:keys [exception fn-processing] :as exception-data} (ex-data e)]
        (when (= fn-processing :poll!)
          (commit-processed-messages! consumer consumer-status))
        (log/error (ex-message e) (dissoc exception-data :exception) exception)))
    (finally
      (.close consumer))))

(defn ^:private consumer-run!
  [{:keys [instance client-id]} config-run]
  (let [consumer-info {:client-id client-id}
        consumer-status (consumer-init consumer-info)]
    (future
      (try
        (log/info "Init consumer-run!" consumer-info)
        (future (consumer-processing instance consumer-status))
        (future (handler-run! config-run consumer-status))
        @(:stoped consumer-status)
        (log/info "Stopedd consumer-run!" consumer-info)
        (catch Exception e
          (.printStackTrace e)
          (log/error "Error function consumer-run!" consumer-info e)
          ((:stop consumer-status)))))
    consumer-status))

(defn consumer-run
  "Execute poll in topics and running handler"
  [kafka-consumer config-run]
  {:pre [(s/valid? ::f-specs/consumer-run config-run)]}
  (consumer-run! kafka-consumer config-run))

(defn create-consumer-component [{:keys [config-consumer config-run] :as consumer}]
  (let [kafka-consumer (create-consumer config-consumer)]
    (subscribe-topics kafka-consumer consumer)
    {:consumer kafka-consumer
     :status (consumer-run! kafka-consumer config-run)}))

(defn create-consumer-component-retry-dlq
  [retry-dlq]
  (-> retry-dlq
      (assoc :topics [(get-in retry-dlq [:config-topic :topic-name])])
      create-consumer-component))

(defn add-consumer-component! [component consumer key config-component]
  (swap! component assoc key consumer)
  config-component)

(defn consumer
  ""
  [{:keys [consumer] :as config-component}]
  {:pre [(s/valid? ::f-specs/consumer-component config-component)]}
  (let [component (atom {})]
    (try
      (let [{:keys [config-run]} consumer
            {auto-retry? :auto-retry?
             consumer-dlq? :consumer-dlq?} config-run]
        (cond-> config-component
          auto-retry? f-retry/init-component-retry-dlq
          auto-retry? (#(add-consumer-component! component
                                                 (create-consumer-component-retry-dlq (:retry %))
                                                 :retry-consumer-component
                                                 %))
          auto-retry? (#(assoc-in %
                                  [:consumer :config-run :handler-exception]
                                  (f-retry/handler-exception-auto-retry config-run %)))
          consumer-dlq? (#(add-consumer-component! component
                                                   (create-consumer-component-retry-dlq (:dlq %))
                                                   :dlq-consumer-component
                                                   %))
          true (#(add-consumer-component! component
                                          (create-consumer-component (:consumer %))
                                          :consumer-component
                                          %)))
        @component)
      (catch Exception e
        (log/error "Component creation error" {:component config-component} e)
        (doseq [[_ v] @component]
          (.close v))
        (throw e)))))

(comment

  (def conff {:consumer {:config-consumer {:bootstrap.servers "localhost:9092"
                                           :group.id "my-group-fortress6"
                                           :auto.offset.reset "earliest"
                                           :enable.auto.commit false}
                         :topics ["TOPIC-1" "TOPIC-2"]
                        ;;  :pattern #"TOP.*"
                         :config-run {:handler (fn [record] #_(prn (.value record)) #_(Thread/sleep 1000) (throw (ex-info "error!" {:record record})))
                                      :handler-exception (fn [record ex] nil #_(prn "DEU ERRO MESMO!!"))
                                      :auto-retry? true
                                      :consumer-dlq? true}}
              :retry {:config-topic {:topic-name "TOPIC-X-RETRY"
                                     :replications 2
                                     :partitions 2}
                      :config-consumer {:bootstrap.servers "localhost:9092"
                                        :group.id "my-group-fortress6"
                                        :auto.offset.reset "earliest"
                                        :max.poll.interval.ms (int 3000)}
                      :retries 3
                      :delay-seg 30
                      :filter-message-retryable (constantly true)}
              :dlq {:config-topic {:topic-name "TOPIC-X-DLQ"
                                   :replications 2
                                   :partitions 2}
                    :config-run {:handler (fn [record] nil #_(prn "CHEGOU NO DLQ"))
                                 :auto-retry? true
                                 :consumer-dlq? true}}})

  (def mc2 (consumer conff))

  ((:stop (:status (:dlq-consumer-component mc2))))
  ((:stop (:status (:retry-consumer-component mc2))))
  ((:stop (:status (:consumer-component mc2))))

  @(:*count (:status (:consumer-component mc2)))

  #_())