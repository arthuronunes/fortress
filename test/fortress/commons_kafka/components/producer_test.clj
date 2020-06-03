(ns fortress.commons-kafka.components.producer-test
  (:require [clojure.test :refer [deftest testing is]]
            [fortress.commons-kafka.components.util :as util]
            [fortress.commons-kafka.components.producer :as f-producer]))

(deftest send!-test 
  (testing "sending message to local kafka"
    (f-producer/send! util/config-broker-local util/topic-teste "id-msg-1423" "message teste")
    true))