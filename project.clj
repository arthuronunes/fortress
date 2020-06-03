(defproject fortress/commons-kafka "1.0.0"
  :description "Library of Kafka Components"
  :target-path "target/%s"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-tools-deps "0.4.5"]
            [s3-wagon-private "1.3.1"]
            [org.glassfish.jaxb/jaxb-runtime "2.3.2"]]
  :repositories [["confluent"      {:url "https://packages.confluent.io/maven/"}]]
  :lein-tools-deps/config {:config-files [:install :project]}
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn])
