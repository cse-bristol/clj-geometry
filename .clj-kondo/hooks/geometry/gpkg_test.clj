(ns hooks.geometry.gpkg-test
  (:require [clj-kondo.hooks-api :as api]))

(defn with-temp-file [{:keys [node]}]
  (let [[sym & body] (rest (:children node))
        new-node (api/list-node
                  (list*
                   (api/token-node 'let)
                   (api/vector-node [sym (api/token-node nil)])
                   body))]
    {:node new-node}))
