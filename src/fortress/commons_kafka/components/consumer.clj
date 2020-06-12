(ns fortress.commons-kafka.components.consumer
  (:require [clojure.walk :as walk]
            [clojure.spec.alpha :as s]
            [fortress.commons-kafka.configs.serdes :as f-serdes]
            [fortress.commons-kafka.configs.specs :as f-specs]
            [fortress.commons-kafka.configs.retry :as f-retry]
            [fortress.commons-kafka.configs.log :as log])
  (:import [org.apache.kafka.clients.consumer
            KafkaConsumer]
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
  (let [*running? (ref true)
        stoped (promise)]
    {:*running? *running?
     :stop #(alter *running? (fn [_]
                               (log/info "Stop consumer-run!" consumer-info)
                               (deliver stoped true)
                               false))
     :stoped stoped
     :*count (ref 0)
     :*messages (ref (clojure.lang.PersistentQueue/EMPTY))
     :*pause? (ref false)}))

(defn ^:private consumer-handler
  [{:keys [handler handler-exception]} record]
  (try
    (handler record)
    (catch Exception e
      (log/error "Error processing message" {:record record} e)
      (when handler-exception
        (handler-exception record e)))))

(defn ^:private next-message
  [consumer-status]
  #_TODO
  #_GET_NEXT_MESSAGE
  #_UPDATE_LIST_MESSAGES
  (alter (:*count consumer-status) inc))

(defn ^:private handler-run!
  [config-run consumer-status]
  (while @(:*running? consumer-status)
    (when-let [record (next-message consumer-status)]
      (consumer-handler config-run record))))

(defn resume-consumer
  [consumer consumer-status]
  #_TODO
  #_RESUME_NO_CONSUMER
  #_ATUALIZAR_REF_*PAUSE?)

(defn pause-consumer
  [consumer consumer-status]
  #_TODO
  #_PAUSE_NO_CONSUMER
  #_ATUALIZAR_REF_*PAUSE?)

(defn message-limit-exceeded?
  [{:keys [*messages]}]
  (> 1000 (count @(*messages))))

(defn add-records
  [records consumer-status]
  #_TODO
  #_ADD_RECORDS_IN_LIST)

(defn ^:private poll! [consumer consumer-status]
  (while @(:*running? consumer-status)
    (let [{:keys [*pause?]} consumer-status
          exceeded? (message-limit-exceeded? consumer-status)]
      (if (not @(*pause?))
        (if exceeded?
          (pause-consumer consumer consumer-status)
          (let [records (.poll consumer (Duration/ofMillis 2000))]
            (add-records records consumer-status)))
        (when (not exceeded?)
          (resume-consumer consumer consumer-status))))))

(defn ^:private consumer-run!
  [{:keys [instance client-id]} config-run]
  (let [consumer-info {:client-id client-id}
        consumer-status (consumer-init consumer-info)]
    (future
      (try
        (log/info "Init consumer-run!" consumer-info)
        (dosync
         (future (poll! instance consumer-status))
         (future (handler-run! config-run consumer-status)))
        @(:stoped consumer-status)
        (catch Exception e
          (.printStackTrace e)
          (log/error "Error consumer-run!" consumer-info e)
          ((:stop consumer-status)))
        (finally
          (.close instance))))
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
                                           :auto.offset.reset "earliest"}
                         :topics ["TOPIC-1" "TOPIC-2"]
                        ;;  :pattern #"TOP.*"
                         :config-run {:handler (fn [record] (prn (.value record)) (throw (ex-info "error!" {:record record})))
                                      :handler-exception (fn [record ex] (prn "DEU ERRO MESMO!!"))
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
                    :config-run {:handler (fn [record] (prn "CHEGOU NO DLQ"))}}})

  (def mc2 (consumer conff))

  ((:stop (:status (:dlq-consumer-component mc2))))
  ((:stop (:status (:retry-consumer-component mc2))))
  ((:stop (:status (:consumer-component mc2))))

  #_())