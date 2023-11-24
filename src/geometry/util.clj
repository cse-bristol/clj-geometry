(ns geometry.util
  "Utility functions for operating on geometries"
  (:require [taoensso.timbre :as log]
            [geometry.core :as g]
            [geometry.noder :as n])
  (:import [org.locationtech.jts.operation.polygonize Polygonizer]
           [org.locationtech.jts.geom Geometry PrecisionModel]))

(defn polygonize
  "Polygonize the lines in the input geometries.
  Returns a new set of polygons. :snapping-scale-factor is a float given to
  PrecisionModel, so for 27700 1.0 is 1m, 0.1 is 100cm and so on.
  "
  [geometries & {:keys [snapping-scale-factor]
            :or   {snapping-scale-factor 10.0}}]
  (let [noded-paths (n/node
                     geometries
                     :precision-model (new PrecisionModel (float snapping-scale-factor)))

        polygonizer (doto (new Polygonizer)
                      (.add noded-paths))
        
        invalid-ring-lines (count (.getInvalidRingLines polygonizer))
        dangles            (count (.getDangles polygonizer))
        cut-edges          (count (.getCutEdges polygonizer))
        polygons           (.getPolygons polygonizer)
        invalid            (remove g/valid? polygons)]
    (log/infof "polygonized paths: %d polygons - %d invalid ring lines - %d dangles - %d cut-edges",
               (count polygons) invalid-ring-lines dangles cut-edges)
    (log/infof "%d invalid polygons" (count invalid))
    polygons))
