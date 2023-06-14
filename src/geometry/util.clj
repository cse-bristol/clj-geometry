(ns geometry.util
  "Utility functions for operating on geometries"
  (:require [geometry.core :as g]))

(defn polygonize [paths & {:keys [snapping-scale-factor]
                           :or {snapping-scale-factor 10.0}}]
  (let [noded-paths (node-paths
                     paths
                     :snapping-scale-factor snapping-scale-factor)
        polygonizer (new Polygonizer)
        _ (.add polygonizer noded-paths)

        invalid-ring-lines (count (.getInvalidRingLines polygonizer))
        dangles (count (.getDangles polygonizer))
        cut-edges (count (.getCutEdges polygonizer))
        polygons (.getPolygons polygonizer)
        invalid (remove valid? polygons)]
    (log/infof "polygonized paths: %d polygons - %d invalid ring lines - %d dangles - %d cut-edges",
               (count polygons) invalid-ring-lines dangles cut-edges)
    (log/infof "%d invalid polygons" (count invalid))
    polygons))
