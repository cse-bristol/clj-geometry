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
  (:require [geometry.core :as g]
            [geometry.index :as index])
  (:import [com.github.davidmoten.rtree2 Entry RTree]
           [com.github.davidmoten.rtree2.geometry Geometries]
           [com.github.davidmoten.rtree2.internal EntryDefault]
           [org.locationtech.jts.geom.prep PreparedGeometryFactory]
           [org.locationtech.jts.geom Geometry]))

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

(defn create ^RTree [xs]
  (RTree/create
   (new java.util.ArrayList
        (->> xs
             (into []
                   (comp
                    (filter identity)
                    (remove g/empty-geom?)
                    (map #(new EntryDefault % (rtree-bounds %)))))))))

(defn entries
  [^RTree index]
  (map #(.value %) (.entries index)))

(defn neighbours
  "Find nearest indexed feature(s) using RTree.nearest() ref
   https://javadoc.io/static/com.github.davidmoten/rtree/0.11/com/github/davidmoten/rtree/RTree.html#nearest-com.github.davidmoten.rtree.geometry.Rectangle-double-int-
   If more than one entry found they are sorted nearest first.
   `q`: query shape
   `range`: max distance of returned entries from a rectangle around q
   `n`: max number of entries to return
   
   Be careful when using an `n` of one - the indexed feature whose
   bounding box is closest to the bounding box of the query geometry
   is not the same as the index feature which is closest to the query
   geometry. It's best to use a higher `n` and take the first entry
   as entries are sorted by actual distance.
   "
  [^RTree index q range n]
  (let [matches (into [] (.nearest index
                                   (rtree-rectangle-bounds q)
                                   (double range) (int n)))]
    (if (= n 1)
      (let [m (first matches)]
        (and m [(.value ^Entry m)]))

      (->> matches
           (sort-by (fn [^Entry x] (g/distance q (.value x))))
           (map (fn [^Entry e] (.value e)))))))

(defn query
  "Query the index for all elements whose envelope intersects the query's envelope,
   or whose envelope is within `radius` of the query's envelope if `radius` is specified."
  ([^RTree index query]
   (let [b (rtree-rectangle-bounds query)]
     (into []
           (map (fn [^Entry e] (.value e)))
           (.search index b))))
  ([^RTree index query radius]
   (let [b (rtree-rectangle-bounds query)]
     (into []
           (map (fn [^Entry e] (.value e)))
           (.search index b radius)))))

(defn- do-query
  "Query an rtree.
     
     `index`: the rtree.
     `q`: the query geometry.
     `op`: the spatial comparison to perform when filtering matches."
  [^RTree index q op]
  (let [b (rtree-rectangle-bounds q)
        matches (into [] (.search index b))
        prepped (PreparedGeometryFactory/prepare (g/geometry q))]
    (keep (fn [^Entry match]
            (when (op prepped (.value match))
              (.value match)))
          matches)))

(defmacro defquery [name doc op]
  `(defn ~name ~doc [^RTree index# query#]
     (do-query index# query# ~op)))

(defquery intersecting
  "Returns a seq of every element in the index that intersects with the query"
  #(.intersects %1 (g/geometry %2)))
(defquery touching
  "Returns a seq of every element in the index that touches the query"
  #(.touches %1 (g/geometry %2)))
(defquery overlapping
  "Returns a seq of every element in the index that overlaps the query"
  #(.overlaps %1 (g/geometry %2)))
(defquery covered-by
  "Returns a seq of every element in the index that is covered entirely by the query"
  #(.covers %1 (g/geometry %2)))

(defquery centroid-intersecting
  "Returns a seq of every element in the index whose centroid intersects with the query"
  #(.intersects %1 (g/centroid-of (g/geometry %2))))
