(ns geometry.index
  "Defines spatial indexing features that can be used with geometry.core

  A spatial index provides an efficient way to spatially search a
  collection of things which have geometry by their proximity (or other geometric predicates)

  Usage is something like

  (let [some-features ... ;; a seq of things which implement g/HasGeometry
        i (index/create some-features)
        
        query-feature ... ;; a feature

        ys (index/neighbours i query-feature 100.0 10)
        ;; ys is a list of up to 10 some-features within 100 meters of query-feature

        zs (index/intersecting i query-feature)
        ;; zs is a list of everything from some-features which intersects query-feature
        ]
        
     )


  Indexes are immutable persistent datastructures, so index/add and index/delete
  return a new index.
  "
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
  "Find nearest indexed feature(s) using RTree.nearest() ref
   https://javadoc.io/static/com.github.davidmoten/rtree/0.11/com/github/davidmoten/rtree/RTree.html#nearest-com.github.davidmoten.rtree.geometry.Rectangle-double-int-
   If more than one entry found they are sorted nearest first.
   `q`: query shape
   `range`: max distance of returned entries from a rectangle around q
   `n`: max number of entries to return
   "
  (let [matches (into [] (.nearest index
                                   (rtree-rectangle-bounds q)
                                   (double range) (int n)))]
    (if (= n 1)
      (let [m (first matches)]
        (and m [(.value ^Entry m)]))
      
      (map (fn [^Entry e] (.value e))
           (sort-by (fn [^Entry x] (g/distance q (.value x)))
                    matches)))))

(defmacro defquery [name doc op]
  `(defn ~name ~doc [^RTree index# query#]
     (let [b# (rtree-rectangle-bounds query#)
           matches# (into [] (.search index# b#))]
       (keep (fn [^Entry e#]
               (when (~op query# (.value e#)) (.value e#)))
             matches#))))

(defquery intersecting
  "Returns a seq of every element in the index that intersects with the query"
  g/intersects?)
(defquery touching
  "Returns a seq of every element in the index that touches the query"
  g/touches?)
(defquery overlapping
  "Returns a seq of every element in the index that overlaps the query"
  g/overlaps?)
(defquery covered-by
  "Returns a seq of every element in the index that is covered entirely by the query"
  g/covers?)

