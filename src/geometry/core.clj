(ns geometry.core
  "Defines core geometry operations around the JTS geometry suite.

  Geometry type things should implement HasGeometry.

  Geometry-changing operations will mostly return an updated version
  of their first argument, via HasGeometry/update-geometry.

  So for example unioning a 'feature' with something else that has
  geometry should return that feature with a unioned geometry.
  
  There are a few exceptions to this, which end with -of, like
  centroid-of.

  This is because it feels weird to have the centroid of a feature
  return a feature.
  "
  (:import
   [org.locationtech.jts.geom.util GeometryFixer]
   [org.locationtech.jts.io WKTReader]
   [org.locationtech.jts.algorithm.hull ConcaveHull]
   [org.locationtech.jts.precision GeometryPrecisionReducer]
   [org.locationtech.jts.operation.distance GeometryLocation]
   [org.locationtech.jts.geom
    LinearRing
    Coordinate Geometry GeometryFactory LineString Point Polygon
    PrecisionModel]))

(def ^:private patched-csf
  "This amended coordinate sequence factory is required so we get
  the right type of coordinate object out; otherwise some code paths
  produce CoordinateXY, which breaks things in parts of jts"
  (let [csf ^org.locationtech.jts.geom.CoordinateSequenceFactory
        (org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory/instance)]
    (reify Object
      org.locationtech.jts.geom.CoordinateSequenceFactory
      (^org.locationtech.jts.geom.CoordinateSequence
       create [_ ^"[Lorg.locationtech.jts.geom.Coordinate;" x]
       ^org.locationtech.jts.geom.CoordinateSequence
       (.create csf ^"[Lorg.locationtech.jts.geom.Coordinate;" x))

      (^org.locationtech.jts.geom.CoordinateSequence create
       [_ ^org.locationtech.jts.geom.CoordinateSequence x]
       (.create csf x))

      (^org.locationtech.jts.geom.CoordinateSequence create
       [_ ^int size ^int dimension]
       (.create csf size (int 3)))

      (^org.locationtech.jts.geom.CoordinateSequence create
       [_ ^int size ^int dimension ^int measures]
       (.create csf size (int 3) measures)))))

(defn geometry-factory [& {:keys [cm-precision srid] :or {srid 27700}}]
  (if cm-precision
    (GeometryFactory. (PrecisionModel. (float (/ 100.0 cm-precision)))
                      srid
                      patched-csf)
    (GeometryFactory. patched-csf)))

(def ^:dynamic ^GeometryFactory *factory*
  (geometry-factory :cm-precision 10))

;; protocol for things with geometry
(defprotocol HasGeometry
  (geometry ^Geometry [g])
  (update-geometry [x g]))

(declare read-wkt)

(extend-type Geometry HasGeometry
             (geometry [g] g)
             (replace-geometry [_ g] g))

(extend-type String HasGeometry
             (geometry [s] (read-wkt s))
             (replace-geometry [_ g] g))

;; functions to make geometries
(defn coordinate? [x] (instance? Coordinate x))

(defn make-coordinate
  ([x]
   (cond
     (coordinate? x) x
     (vector? x) (make-coordinate (first x) (second x))
     (point? x)  (.getCoordinate (geometry x))
     :else (throw (IllegalArgumentException. (str "Unsupported type for make-coordinate "
                                                  (class x))))))
  ([^double x ^double y] (Coordinate. x y)))

(defn make-coordinates ^"[Lorg.locationtech.jts.geom.Coordinate;"
  [xs]
  (cond
    (instance? HasGeometry xs) (.getCoordinates (geometry xs))
    (seqable? xs) (into-array Coordinate (map make-coordinate xs))

    :else (throw (IllegalArgumentException. (str "Unsupported type for make-coordinates "
                                                 (class xs))))))

(defn make-point
  ([x]   (.createPoint ^GeometryFactory *factory* (make-coordinate x)))
  ([x y] (.createPoint ^GeometryFactory *factory* (make-coordinate x y))))

(defn make-line-string [xys]
  (if (line-string? xys)
    xys (.createLineString ^GeometryFactory *factory* (make-coordinates xys))))

(defn make-linear-ring [xys]
  (.createLinearRing ^GeometryFactory *factory*
                     (make-coordinates xys)))

(defn make-polygon
  ([xys]
   (if (polygon? xys)
     xys (.createPolygon ^GeometryFactory *factory* (make-coordinates xys))))
  
  ([xys rings]
   (.createPolygon ^GeometryFactory *factory*
                   (make-linear-ring xys)
                   (into-array LinearRing (map make-linear-ring rings)))))

(defn make-multi-line-string [xyss]
  (if (multi-line-string? xyss) xyss
      (.createMultiLineString ^GeometryFactory
                              *factory*
                              (into-array LineString (map make-line-string xyss)))))

(defn make-multi-point [xys]
  (if (multi-point? xys) xys
      (.createMultiPoint ^GeometryFactory
                         *factory*
                         ^"[Lorg.locationtech.jts.geom.Point;"
                         (into-array Point (map make-point xys)))))

(defn make-multi-polygon [gs]
  (.createMultiPolygon
   ^GeometryFactory
   *factory* (into-array Polygon (map make-polygon gs))))

(defn make-collection [gs]
  (.createGeometryCollection
   ^GeometryFactory *factory* (into-array Geometry gs)))

;; functions to see what things are
(defn geometry-type [g]
  (let [t (.getGeometryType (geometry g))]
    (case t
      "Point"              :point 
      "MultiPoint"         :multi-point
      "Polygon"            :polygon
      "MultiPolygon"       :multi-polygon
      "LineString"         :line-string
      "MultiLineString"    :multi-line-string
      "GeometryCollection" :geometry-collection
      :unknown)))

;; TODO should these call geometry? since that is a question about a thing
;; that has a geometry, rather than a geometry.
(defn point? [g] (= "Point" (.getGeometryType (geometry g))))
(defn line-string? [g] (= "LineString" (.getGeometryType (geometry g))))
(defn polygon? [g] (= "Polygon" (.getGeometryType (geometry g))))
(defn multi-point? [g] (= "MultiPoint" (.getGeometryType (geometry g))))
(defn multi-line-string? [g] (= "MultiLineString" (.getGeometryType (geometry g))))
(defn multi-polygon? [g] (= "MultiPolygon" (.getGeometryType (geometry g))))
(defn collection? [g] (= "GeometryCollection" (.getGeometryType (geometry g))))
(defn single?
  "Is the geometry of g either a point, polygon or linestring?"
  [g]
  (let [t (.getGeometryType (geometry g))]
    (or (= t "Point")
        (= t "Polygon")
        (= t "LineString"))))

;; core geometry operations
(defn union
  ([g]   (update-geometry g (.union (geometry g))))
  ([a b] (update-geometry a (.union (geometry a) (geometry b)))))

(defn intersection
  ([g]   (update-geometry g (.intersection (geometry g))))
  ([a b] (update-geometry a (.intersection (geometry a) (geometry b)))))

(defn difference
  "geometry A minus geometry B"
  [a b]
  (update-geometry a (.difference (geometry a) (geometry b))))

(defn buffer [g ^double r]
  (update-geometry g (.buffer (geometry g) r)))

(defn length ^double [g] (.getLength (geometry g)))
(defn area ^double [g] (.getArea (geometry g)))
(defn intersects? ^Boolean [a b] (.intersects (geometry a) (geometry b)))
(defn touches? ^Boolean [a b] (.touches (geometry a) (geometry b)))
(defn covers? ^Boolean [a b] (.covers (geometry a) (geometry b)))
(defn overlaps? ^Boolean [a b] (.overlaps (geometry a) (geometry b)))
(defn distance ^double [a b] (.distance (geometry a) (geometry b)))
(defn valid? ^Boolean [g] (.isValid (geometry g)))
(defn empty-geom? ^Boolean [g] (.isEmpty (geometry g)))
(defn relates? ^Boolean [a b ^String m] (.relate (geometry a) (geometry b) m))

;; other operations
(defn make-valid [g]
  (if (valid? g) g
      (let [buffed (buffer g 0.0)]
        (if (valid? buffed) buffed
            (update-geometry buffed (GeometryFixer/fix (geometry buffed)))))))

(defn fill-holes [g]
  (-> g
      (exterior-ring)
      (make-coordinates)
      (make-polygon)
      (->> (update-geometry g))))

;; TODO should these update-geometry?
(defn centroid-of      [g] (.getCentroid (geometry g)))
(defn exterior-ring-of [g] (.getExteriorRing (geometry g)))
(defn convex-hull-of   [g] (.convexHull (geometry g)))
;; (defn holes-of         [g] TODO)
(defn concave-hull-of
  "`length-ratio`: Maximum Edge Length Ratio - determine the Maximum Edge Length
   as a fraction of the difference between the longest and shortest edge lengths
   in the Delaunay Triangulation.
   A value of 1 produces the convex hull; a value of 0 produces maximum concaveness."
  [g ^double length-ratio]
  {:pre [(<= 0.0 length-ratio 1.0)]}
  (ConcaveHull/concaveHullByLengthRatio (geometry g) length-ratio))

(defn geometries
  "Get a collection of the contained geometries within g;
  on geometry collections this may still return multipart geoms.
  See also simple-geometries if you want to flatten something"
  [g]
  (let [g (geometry g)]
    (loop [iter 0
           geoms []]
      (if (< iter (.getNumGeometries g))
        (recur (inc iter) (conj geoms (.getGeometryN g iter)))
        geoms))))

(defn single-geometries
  "Get a collection of simple geometries within g (optionally of certain type(s))"
  ([g]
   (let [g (geometry g)]
     (loop [iter 0
            geoms []]
       (if (< iter (.getNumGeometries g))
         (let [gn (.getGeometryN g iter)]
           (if (single? gn)
             (recur (inc iter) (conj geoms gn))
             (recur (inc iter) (into geoms (single-geometries gn)))))
         geoms))))
  ([g type]
   (let [type (if (keyword? type) #{type} (set type))]
     (filter #(comp type (geometry-type %)) (single-geometries g)))))


(defn write-wkt [g] (.toText (geometry g)))

(defn read-wkt [^String wkt]
  (.read (WKTReader. ^GeometryFactory *factory*) wkt))

(defn change-precision
  "scale-factor is the number of cells in the grid, e.g.
   scale-factor of 10 in a metre-based CSR means precision of 0.1m"

  ;; TODO should this set the precisionmodel as well?
  [g ^double scale-factor]
  (update-geometry
   g
   (GeometryPrecisionReducer/reduce
    (geometry g)
    (PrecisionModel. scale-factor))))

(defn set-user-data!
  "Mutates the user-data in the geometry associated with x"
  [g x]
  (let [g' (geometry g)]
    (.setUserData g' x)
    (update-geometry g g')))

(defn user-data [g] (.getUserData (geometry g)))

(defn closest-point-on
  "Return the closest point on a to b"
  [a b]
  (let [op (org.locationtech.jts.operation.distance.DistanceOp.
            (geometry a) (geometry b))
        [l1 _l2] (.nearestLocations op)]
    (make-point (.getCoordinate ^GeometryLocation l1))))
