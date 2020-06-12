(ns fortress.commons-kafka.utils)

(defn coalesce [v1 v2]
  (first (keep identity [v1 v2])))