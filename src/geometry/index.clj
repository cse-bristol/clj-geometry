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

  There are two kinds of index, which the query functions here work on alike:

  - `create` makes an index which is a persistent datastructure, so index/add and
    index/delete return a new index.
  - `create-static` makes an index which can't be changed once made. It's quicker to
    make and smaller, so it suits a large index which is made once and queried many
    times. It can also index things which don't have geometry themselves, given a
    function to find their geometry.
  "
  (:require [geometry.core :as g])
  (:import [com.github.davidmoten.rtree2 Entry RTree]
           [com.github.davidmoten.rtree2.geometry Geometries]
           [com.github.davidmoten.rtree2.internal EntryDefault]
           [java.util List]
           [org.locationtech.jts.geom.prep PreparedGeometry PreparedGeometryFactory]
           [org.locationtech.jts.geom Envelope Geometry]
           [org.locationtech.jts.index.strtree STRtree]))

(defn- rtree-rectangle-bounds ^com.github.davidmoten.rtree2.geometry.Rectangle [g]
  (let [g (g/geometry g)
        bounds (.getEnvelopeInternal g)
        minx (.getMinX bounds)
        maxx (.getMaxX bounds)
        miny (.getMinY bounds)
        maxy (.getMaxY bounds)
        ]
    (Geometries/rectangle minx miny maxx maxy)))

(defn- rtree-bounds ^com.github.davidmoten.rtree2.geometry.Geometry [g]
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

(defprotocol ^:private Index
  (-search [index q radius]
    "The elements whose envelope intersects the envelope of `q`, or is within `radius` of
     it if `radius` is not nil")
  (-geometry-of [index x]
    "The geometry of `x`, an element of the index")
  (-entries [index]
    "All the elements of the index"))

(extend-type RTree
  Index
  (-search [index q radius]
    (let [b (rtree-rectangle-bounds q)]
      (into []
            (map (fn [^Entry e] (.value e)))
            (if radius
              (.search index b (double radius))
              (.search index b)))))
  (-geometry-of [_ x] (g/geometry x))
  (-entries [index]
    (map #(.value ^Entry %) (.entries index))))

(defn- flatten-items
  "The items in a tree of java Lists, from `STRtree/itemsTree`"
  [tree]
  (if (instance? List tree)
    (mapcat flatten-items tree)
    [tree]))

(deftype StaticIndex [^STRtree tree geometry-fn]
  Index
  (-search [_ q radius]
    (let [envelope (Envelope. (.getEnvelopeInternal ^Geometry (g/geometry q)))]
      (if radius
        (let [radius (double radius)]
          (into []
                (filter #(<= (.distance envelope
                                        (.getEnvelopeInternal ^Geometry (geometry-fn %)))
                             radius))
                (.query tree (doto (Envelope. envelope) (.expandBy radius)))))
        (vec (.query tree envelope)))))
  (-geometry-of [_ x] (geometry-fn x))
  (-entries [_] (flatten-items (.itemsTree tree))))

(def EMPTY (RTree/create))

(defn- check-persistent [index]
  (when-not (instance? RTree index)
    (throw (ex-info "This index can't be changed: it was made by create-static"
                    {:index index}))))

(defn add [index x]
  (check-persistent index)
  (.add ^RTree index x (rtree-bounds x)))

(defn delete [index x]
  (check-persistent index)
  (.delete ^RTree index x (rtree-bounds x)))

(defn create ^RTree [xs]
  (RTree/create
   (new java.util.ArrayList
        (->> xs
             (into []
                   (comp
                    (filter identity)
                    (remove g/empty-geom?)
                    (map #(new EntryDefault % (rtree-bounds %)))))))))

(defn create-static
  "An index of `xs` which can't be changed once made (see the namespace docstring).

   `:geometry` is a function giving the JTS Geometry of each of `xs`, by default
   `geometry.core/geometry`. With it, `xs` can be anything, e.g. indices into a vector
   of things with geometry, which keeps the index small."
  [xs & {:keys [geometry] :or {geometry g/geometry}}]
  (let [tree (STRtree.)]
    (doseq [x xs
            :when (some? x)
            :let [^Geometry geom (geometry x)]
            :when (and geom (not (.isEmpty geom)))]
      (.insert tree (.getEnvelopeInternal geom) x))
    ;; an STRtree builds itself on the first query, which isn't safe to do on several
    ;; threads at once, so build it now
    (.build tree)
    (StaticIndex. tree geometry)))

(defn entries
  [index]
  (-entries index))

(defn query
  "Query the index for all elements whose envelope intersects the query's envelope,
   or whose envelope is within `radius` of the query's envelope if `radius` is specified."
  ([index query]
   (if (g/empty-geom? query)
     []
     (-search index query nil)))
  ([index query ^double radius]
   (if (g/empty-geom? query)
     []
     (-search index query radius))))

(defn- distances-within
  "[distance element] for each element of the index within `range` of `q`"
  [index q range]
  (if (g/empty-geom? q)
    []
    (let [range (double range)
          q     (g/geometry q)]
      (into []
            (keep (fn [x]
                    (let [d (g/distance q (-geometry-of index x))]
                      (when (<= d range) [d x]))))
            (-search index q range)))))

(defn within-distance
  "Every element of the index whose geometry is within `range` of `q`, nearest first.

   Unlike `neighbours`, this uses the actual distance to each element, not to its
   bounding box, and has no limit on how many it finds."
  [index q range]
  (->> (distances-within index q range)
       (sort-by first)
       (mapv second)))

(defn nearest
  "The element of the index nearest to `q`, or nil if none is within `range`.

   Unlike `neighbours` with a small `n`, this is exact. The order an index finds
   elements in is arbitrary, so where several are equally near this takes the one
   with the least `(tie-key element)`, if `tie-key` is given."
  ([index q range]
   (first (within-distance index q range)))
  ([index q range tie-key]
   (->> (distances-within index q range)
        (sort-by (fn [[d x]] [d (tie-key x)]))
        (first)
        (second))))

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

   On an index from `create-static` this is exact: the `n` nearest by actual distance.
   "
  [index q range n]
  (cond
    (g/empty-geom? q)
    []

    (instance? StaticIndex index)
    (take n (within-distance index q range))

    :else
    (let [matches (into [] (.nearest ^RTree index
                                     (rtree-rectangle-bounds q)
                                     (double range) (int n)))]
      (if (= n 1)
        (let [m (first matches)]
          (and m [(.value ^Entry m)]))

        (->> matches
             (sort-by (fn [^Entry x] (g/distance q (.value x))))
             (map (fn [^Entry e] (.value e))))))))

(defn- do-query
  "Query an index.
     
     `index`: the index.
     `q`: the query geometry.
     `op`: the spatial comparison to perform when filtering matches, a function of the
     prepared query geometry and the geometry of a match."
  [index q op]
  (if (g/empty-geom? q)
    []
    (let [prepped (PreparedGeometryFactory/prepare (g/geometry q))]
      (filter (fn [x] (op prepped (-geometry-of index x)))
              (-search index q nil)))))

(defmacro defquery [name doc op]
  `(defn ~name ~doc [index# query#]
     (do-query index# query# ~op)))

(defquery intersecting
  "Returns a seq of every element in the index that intersects with the query"
  #(.intersects ^PreparedGeometry %1 ^Geometry %2))
(defquery touching
  "Returns a seq of every element in the index that touches the query"
  #(.touches ^PreparedGeometry %1 ^Geometry %2))
(defquery overlapping
  "Returns a seq of every element in the index that overlaps the query"
  #(.overlaps ^PreparedGeometry %1 ^Geometry %2))
(defquery covered-by
  "Returns a seq of every element in the index that is covered entirely by the query"
  #(.covers ^PreparedGeometry %1 ^Geometry %2))

(defquery centroid-intersecting
  "Returns a seq of every element in the index whose centroid intersects with the query"
  #(.intersects ^PreparedGeometry %1 (g/centroid-of %2)))
