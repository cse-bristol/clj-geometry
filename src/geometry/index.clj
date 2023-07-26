(ns geometry.index
  "Defines spatial indexing features that can be used with geometry.core"
  (:import
   [org.locationtech.jts.geom Geometry Envelope Coordinate]
   [com.github.davidmoten.rtree2 RTree Entry]
   [com.github.davidmoten.rtree2.geometry Geometries])
  (:require [geometry.core :as g]))

(defn- rtree-rectangle-bounds ^com.github.davidmoten.rtree2.geometry.Rectangle [g]
  (let [g (g/geometry g)
        bounds (.getEnvelopeInternal g)
        minx (.getMinX bounds)
        maxx (.getMaxX bounds)
        miny (.getMinY bounds)
        maxy (.getMaxY bounds)
        ]
    (Geometries/rectangle minx miny maxx maxy)))

(defn- rtree-bounds [g]
  (let [g (g/geometry g)
        bounds (.getEnvelopeInternal g)
        minx (.getMinX bounds)
        maxx (.getMaxX bounds)
        miny (.getMinY bounds)
        maxy (.getMaxY bounds)
        ]
    (if (and (== minx maxy) (== miny maxy))
      (Geometries/point minx miny)
      (Geometries/rectangle minx miny maxx maxy))))

(def EMPTY (RTree/create))

(defn add [^RTree index x]
  (.add index x (rtree-bounds x)))

(defn delete [^RTree index x]
  (.delete index x (rtree-bounds x)))

(defn create [xs] (reduce add EMPTY xs))

(defn neighbours [^RTree index q range n]
  (let [matches (into [] (.nearest index
                                   (rtree-rectangle-bounds q)
                                   (double range) (int n)))]
    (if (= n 1)
      (let [m (first matches)]
        (and m [(.value ^Entry m)]))
      
      (map (fn [^Entry e] (.value e))
           (sort-by (fn [^Entry x] (g/distance q (.value x)))
                    matches)))))

(defn intersection [^RTree index query]
  (let [b (rtree-rectangle-bounds query)
        matches (into [] (.search index b))]
    (keep (fn [^Entry e]
            (when (g/intersects? query (.value e))
              (.value e)))
          matches)))


