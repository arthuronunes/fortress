(ns fortress.commons-kafka.components.producer-test
  (:require [clojure.test :refer [deftest testing is]]
            [fortress.commons-kafka.components.util :as util]
            [fortress.commons-kafka.components.producer :as f-producer]
            [fortress.commons-kafka.configs.data :as f-data]))

(deftest send!-test 
  (testing "sending message to local kafka"
    (f-producer/send! util/config-broker-local util/topic-teste2 "id-msg-1423" (f-data/map->string-json {:a 1 :v 2 :b {:a 1234}}))
    true))