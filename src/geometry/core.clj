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
  (:import [org.locationtech.jts.algorithm MinimumBoundingCircle]
           [org.locationtech.jts.algorithm.hull ConcaveHull]
           [org.locationtech.jts.geom
            Coordinate
            Geometry
            GeometryFactory
            LineString
            LinearRing
            Point
            Polygon
            PrecisionModel
            TopologyException]
           [org.locationtech.jts.geom.util GeometryFixer]
           [org.locationtech.jts.io WKTReader]
           [org.locationtech.jts.noding SegmentStringDissolver SegmentStringUtil]
           [org.locationtech.jts.noding.snapround SnapRoundingNoder]
           [org.locationtech.jts.operation.buffer BufferOp BufferParameters]
           [org.locationtech.jts.operation.distance GeometryLocation]
           [org.locationtech.jts.operation.linemerge LineMerger]
           [org.locationtech.jts.operation.overlay OverlayOp]
           [org.locationtech.jts.operation.overlayng OverlayNGRobust]
           [org.locationtech.jts.operation.polygonize Polygonizer]
           [org.locationtech.jts.precision GeometryPrecisionReducer]
           [org.locationtech.jts.densify Densifier]))

(def ^:private ^org.locationtech.jts.geom.CoordinateSequenceFactory patched-csf
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

(defn fixed-precision-model ^PrecisionModel [cm-precision]
  (PrecisionModel. (float (/ 100.0 cm-precision))))

(defn geometry-factory [& {:keys [cm-precision srid] :or {srid 27700}}]
  (if cm-precision
    (GeometryFactory. (PrecisionModel. (float (/ 100.0 cm-precision)))
                      srid
                      patched-csf)
    (GeometryFactory. patched-csf)))

(def ^:dynamic ^GeometryFactory *factory*
  "The default GeometryFactory. Doesn't set any PrecisionModel, as this only
   causes all coordinates to be snapped to a grid, rather than running whole
   geometries through e.g. the GeometryPrecisionReducer. 
   
   As a result it is only safe if you already know that the geometries all 
   have a certain precision - in other cases it will cause valid geometries
   (that could remain valid at the requested precision using the 
   GeometryPrecisionReducer) to become invalid."
  (geometry-factory))

;; protocol for things with geometry
(defprotocol HasGeometry
  (geometry ^org.locationtech.jts.geom.Geometry [g])
  (update-geometry [x g]))

(declare read-wkt)

(extend-type Geometry HasGeometry
             (geometry [g] g)
             (update-geometry [_ g] g))

(extend-type String HasGeometry
             (geometry [s] (read-wkt s))
             (update-geometry [_ g] g))

;; functions to make geometries
(defn coordinate? [x] (instance? Coordinate x))
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
      "LinearRing"         :linear-ring
      :unknown)))

(defn geometry? 
  "Tests if a thing is an actual JTS geometry. Features and Strings will return
   false."
  [x] 
  (instance? Geometry x))

(defn has-geometry?
  "Tests if a thing implements the HasGeometry protocol. This does not mean
   they actually have a geometry - e.g. all strings will return true."
  [x] 
  (satisfies? HasGeometry x))

;; TODO should these call geometry? since that is a question about a thing
;; that has a geometry, rather than a geometry
(defn point? [g] (and (satisfies? HasGeometry g)
                      (= "Point" (.getGeometryType (geometry g)))))
(defn line-string? [g] (and (satisfies? HasGeometry g)
                            (= "LineString" (.getGeometryType (geometry g)))))
(defn polygon? [g] (and (satisfies? HasGeometry g)
                        (= "Polygon" (.getGeometryType (geometry g)))))
(defn multi-point? [g] (and (satisfies? HasGeometry g)
                            (= "MultiPoint" (.getGeometryType (geometry g)))))
(defn multi-line-string? [g] (and (satisfies? HasGeometry g)
                                  (= "MultiLineString" (.getGeometryType (geometry g)))))
(defn multi-polygon? [g] (and (satisfies? HasGeometry g)
                              (= "MultiPolygon" (.getGeometryType (geometry g)))))
(defn collection? [g] (and (satisfies? HasGeometry g)
                           (= "GeometryCollection" (.getGeometryType (geometry g)))))
(defn single?
  "Is the geometry of g either a point, polygon or linestring?"
  [g]
  (and (satisfies? HasGeometry g)
       (let [t (.getGeometryType (geometry g))]
         (or (= t "Point")
             (= t "Polygon")
             (= t "LineString")
             (= t "LinearRing")))))
(defn multi?
  "Is the geometry of g either a multi-point, multi-polygon, multi-linestring or geometry collection?"
  [g]
  (and (satisfies? HasGeometry g)
       (let [t (.getGeometryType (geometry g))]
         (or (= t "MultiPoint")
             (= t "MultiPolygon")
             (= t "MultiLineString")
             (= t "GeometryCollection")))))

(defn ^Coordinate make-coordinate
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
    (satisfies? HasGeometry xs) (.getCoordinates (geometry xs))
    (seqable? xs) (into-array Coordinate (map make-coordinate xs))

    :else (throw (IllegalArgumentException. (str "Unsupported type for make-coordinates "
                                                 (class xs))))))

(defn make-point
  ([x]   (.createPoint ^GeometryFactory *factory* ^Coordinate (make-coordinate x)))
  ([x y] (.createPoint ^GeometryFactory *factory* ^Coordinate (make-coordinate x y))))

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
   ^GeometryFactory *factory* (into-array Geometry (map geometry gs))))

;; core geometry operations


(defn valid? ^Boolean [g] (.isValid (geometry g)))

(defn make-valid [g]
  (if (valid? g) g
      (let [buffed (if (#{:polygon :multi-polygon} (geometry-type g))
                     (.buffer (geometry g) 0.0)
                     g)]
        (if (valid? buffed)
          (update-geometry g buffed)
          (update-geometry g (GeometryFixer/fix (geometry g)))))))

(def end-cap-styles {:round 1 :flat 2 :square 3})
(def join-styles {:round 1 :mitre 2 :bevel 3})

(defn buffer
  ([g ^double r]
   (update-geometry g (make-valid (.buffer (geometry g) (double r)))))

  ([g r quad-segs end-cap-style join-style]
   (buffer g r quad-segs end-cap-style join-style 5.0))

  ([g r quad-segs end-cap-style join-style mitre-limit]
   (update-geometry
    g
    (make-valid
     (BufferOp/bufferOp (geometry g)
                        (double r)
                        (BufferParameters. quad-segs
                                           (end-cap-styles end-cap-style)
                                           (join-styles join-style)
                                           mitre-limit))))))

(defn intersects? ^Boolean [a b] (.intersects (geometry a) (geometry b)))
(defn touches? ^Boolean [a b] (.touches (geometry a) (geometry b)))
(defn covers? ^Boolean [a b] (.covers (geometry a) (geometry b)))
(defn overlaps? ^Boolean [a b] (.overlaps (geometry a) (geometry b)))
(defn contains? ^Boolean [a b] (.contains (geometry a) (geometry b)))
(defn relates? ^Boolean [a b ^String m] (.relate (geometry a) (geometry b) m))

(defn length ^double [g] (.getLength (geometry g)))
(defn area ^double [g] (.getArea (geometry g)))
(defn distance ^double [a b] (.distance (geometry a) (geometry b)))
(defn empty-geom? ^Boolean [g] (.isEmpty (geometry g)))

;; other operations

(defn exterior-ring-of [g] (.getExteriorRing ^Polygon (geometry g)))

(defn fill-holes [g]
  (-> g
      (exterior-ring-of)
      (make-coordinates)
      (make-polygon)
      (->> (update-geometry g))))

(defn centroid-of [g] (.getCentroid (geometry g)))

(defn to-centroid 
  "If `g` is a feature, return a feature with the centroid as the geometry.
   Otherwise identical to `centroid-of`"
  [g] 
  (update-geometry g (.getCentroid (geometry g))))

(defn boundary-of 
  "See https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/Geometry.html#getBoundary--"
  [g] (.getBoundary (geometry g)))

(defn to-boundary 
  "If `g` is a feature, return a feature with the boundary as the geometry.
   Otherwise identical to `boundary-of`"
  [g] 
  (update-geometry g (.getBoundary (geometry g))))

(defn interior-point-of ^Point [g]
  (.getInteriorPoint (geometry g)))

(defn bounding-box [g]
  (let [envelope (.getEnvelopeInternal (geometry g))]
    {:xmin (.getMinX envelope)
     :ymin (.getMinY envelope)
     :xmax (.getMaxX envelope)
     :ymax (.getMaxY envelope)}))

(defn min-x [g] (.getMinX (.getEnvelopeInternal (geometry g))))
(defn min-y [g] (.getMinY (.getEnvelopeInternal (geometry g))))
(defn max-x [g] (.getMaxX (.getEnvelopeInternal (geometry g))))
(defn max-y [g] (.getMaxY (.getEnvelopeInternal (geometry g))))

(defn convex-hull-of [g] (.convexHull (geometry g)))

(defn concave-hull-of
  "`length-ratio`: Maximum Edge Length Ratio - determine the Maximum Edge Length
   as a fraction of the difference between the longest and shortest edge lengths
   in the Delaunay Triangulation.
   A value of 1 produces the convex hull; a value of 0 produces maximum concaveness."
  [g ^double length-ratio]
  {:pre [(<= 0.0 length-ratio 1.0)]}
  (ConcaveHull/concaveHullByLengthRatio (geometry g) length-ratio))

(defn minimum-bounding-circle-of
  "See https://locationtech.github.io/jts/javadoc/org/locationtech/jts/algorithm/MinimumBoundingCircle.html"
  [g]
  (MinimumBoundingCircle. (geometry g)))

(defn geometries
  "Get a collection of the contained geometries within g;
  on geometry collections this may still return multipart geoms.
  See also single-geometries if you want to flatten something"
  [g]
  (let [g (geometry g)]
    (loop [iter 0
           geoms []]
      (if (< iter (.getNumGeometries g))
        (recur (inc iter) (conj geoms (.getGeometryN g iter)))
        geoms))))

(defn single-geometries
  "Get a collection of single geometries within g (optionally of certain type(s))"
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
     (filter #(-> % (geometry-type) (type)) (single-geometries g)))))

(defn polygons-of
  "Get a collection of only the polygons within g, as geometries"
  [g] 
  (single-geometries g :polygon))

(defn polygons 
  "Get a collection of only the polygons within `g`, with each polygon as a
   copy of the input Feature (if `g` is a Feature)"
  [g]
  (mapv #(update-geometry g %) (polygons-of g)))

(defn line-strings-of
  "Get a collection of only the line-strings and linear-rings within g"
  [g]
  (single-geometries g #{:line-string :linear-ring}))

(defn line-strings
  "Get a collection of only the line-strings and linear-rings within `g`, with 
   each geometry as a copy of the input Feature (if `g` is a Feature)"
  [g]
  (mapv #(update-geometry g %) (line-strings-of g)))

(defn points-of
  "Get a collection of only the points within g"
  [g]
  (single-geometries g :point))

(defn points
  "Get a collection of only the points within `g`, with 
   each geometry as a copy of the input Feature (if `g` is a Feature)"
  [g]
  (mapv #(update-geometry g %) (points-of g)))

;; overlay operations

(defn- overlay [a b op]
  (try
    (update-geometry a (make-valid (OverlayNGRobust/overlay (geometry a) (geometry b) op)))
    (catch TopologyException _
      (update-geometry a (make-valid (OverlayNGRobust/overlay (make-valid (geometry a)) (make-valid (geometry b)) op))))))

(defn union
  "Uses JTS' OverlayNGRobust to avoid TopologyExceptions due to coordinates 
   whose precision approaches floating point limits. If this still fails,
   will call `make-valid` on the inputs and retry.
   
   Outputs will have `made-valid` called on them."
  ([g]
   (try
     (update-geometry g (OverlayNGRobust/union (geometry g)))
     (catch TopologyException _
       (update-geometry g (OverlayNGRobust/union ^Geometry (make-valid (geometry g)))))))
  ([a b]
   (overlay a b OverlayOp/UNION)))

(defn intersection
  "Uses JTS' OverlayNGRobust to avoid TopologyExceptions due to coordinates 
   whose precision approaches floating point limits. If this still fails,
   will call `make-valid` on the inputs and retry.
   
   Outputs will have `made-valid` called on them."
  ([a b]
   (overlay a b OverlayOp/INTERSECTION)))

(defn difference
  "Geometry A minus geometry B.
   
   Uses JTS' OverlayNGRobust to avoid TopologyExceptions due to coordinates 
   whose precision approaches floating point limits. If this still fails,
   will call `make-valid` on the inputs and retry.
   
   Outputs will have `made-valid` called on them."
  [a b]
  (overlay a b OverlayOp/DIFFERENCE))

(defn sym-difference
  "Uses JTS' OverlayNGRobust to avoid TopologyExceptions due to coordinates 
   whose precision approaches floating point limits. If this still fails,
   will call `make-valid` on the inputs and retry.
   
   Outputs will have `made-valid` called on them."
  [a b]
  (overlay a b OverlayOp/SYMDIFFERENCE))

(defn linearize
  "Convert geometry to a collection of line-strings and linear-rings. Multipart
   geometries will be expanded; polygons will be converted to their boundaries."
  [g]
  (case (geometry-type g)
    :polygon             [(boundary-of g)]
    :multi-polygon       (map boundary-of (single-geometries g))
    :line-string         [g]
    :linear-ring         [g]
    :multi-line-string   (single-geometries g)
    :geometry-collection (mapcat linearize (single-geometries g))
    []))

(defn line-merge
  [g]
  (let [line-merger (new LineMerger)]
    (.add line-merger (geometry g))
    (.getMergedLineStrings line-merger)))

(defn holes-of [g]
  (->> (polygons (geometry g))
       (mapcat
        (fn [^Polygon geom]
          (for [i (range (.getNumInteriorRing geom))]
            (make-polygon (.getCoordinates (.getInteriorRingN geom i))))))
       (filter identity)))

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
   (let [pm (new PrecisionModel (float scale-factor))
         gpr (doto (new GeometryPrecisionReducer pm)
               (.setChangePrecisionModel true))]
     (.reduce gpr (geometry g)))))

(defn set-user-data!
  "Mutates the user-data in the geometry associated with x.
   Consider using a Feature instead unless a JTS API you are using means there
   is no other way of passing data alongside a geometry (for example, this is
   the case in the noder)"
  [g x]
  (let [g' (geometry g)]
    (.setUserData g' x)
    (update-geometry g g')))

(defn user-data [g] (.getUserData (geometry g)))

(defn closest-points-on
  "Return the closest points between a & b in same order"
  [a b]
  (let [op (org.locationtech.jts.operation.distance.DistanceOp.
            (geometry a) (geometry b))
        [l1 l2] (.nearestLocations op)]
    [(make-point (.getCoordinate ^GeometryLocation l1))
     (make-point (.getCoordinate ^GeometryLocation l2))]))

(defn endpoints-of [g]
  (when (line-string? g)
    (let [g (geometry g)
          c (.getCoordinates g)]
      [(make-point (first c)) (make-point (last c))])))

(defn srid [g] (.getSRID (geometry g)))

(defn ^Coordinate coordinate [g] (.getCoordinate (geometry g)))

(defn split-line
  "Split a thing having linear geometry at a thing having point geometry.
  Returns two new linestrings. User data is copied from the original linestring."
  [line point]
  (let [line (geometry line)
        point (geometry point)
        [^org.locationtech.jts.operation.distance.GeometryLocation gl _]
        (.nearestLocations
         (org.locationtech.jts.operation.distance.DistanceOp. line point))
        
        coordinates (.getCoordinates line)
        split-position (.getSegmentIndex gl)
        split-coordinate (.getCoordinate point)
        [c-start c-end] (split-at (inc split-position) coordinates)]
    
    [(-> (make-line-string (concat c-start [split-coordinate]))
         (set-user-data! (user-data line)))
     (-> (make-line-string (concat [split-coordinate] c-end))
         (set-user-data! (user-data line)))]))

(defn- node-paths
  "Basic noder for use in `polygonize`. See geometry.noder for
   public noder.
   
   From JTS docs: To specify 3 decimal places of precision,
   use a scale factor of 1000.
   To specify -3 decimal places of precision (i.e. rounding to
   the nearest 1000), use a scale factor of 0.001."
  ^Geometry [paths snapping-scale-factor]
  (let [paths (map geometry paths)
        pm (new PrecisionModel (float snapping-scale-factor))
        noder (new SnapRoundingNoder pm)
        noded-paths (mapcat #(SegmentStringUtil/extractSegmentStrings %) paths)
        noded-paths (do (.computeNodes noder noded-paths)
                        (.getNodedSubstrings noder))
        dissolver (new SegmentStringDissolver)
        noded-paths (do (.dissolve dissolver noded-paths)
                        (.getDissolved dissolver))]
    (SegmentStringUtil/toGeometry noded-paths *factory*)))

(defn polygonize
  "From JTS docs: To specify 3 decimal places of precision,
   use a scale factor of 1000.
   To specify -3 decimal places of precision (i.e. rounding to
   the nearest 1000), use a scale factor of 0.001."
  [paths & {:keys [snapping-scale-factor]
            :or {snapping-scale-factor 10.0}}]
  (let [paths (mapcat linearize paths)
        noded-paths (node-paths paths snapping-scale-factor)
        polygonizer (doto (new Polygonizer)
                      (.add noded-paths))
        polygons (.getPolygons polygonizer)]
    polygons))

(defn cut-polygon [polygon paths &
                   {:keys [snapping-scale-factor]
                    :or {snapping-scale-factor 10.0}}]
  (if (empty? paths)
    [polygon]
    (let [holes (make-multi-polygon (holes-of polygon))
          parts (-> paths
                    (conj (boundary-of polygon))
                    (polygonize :snapping-scale-factor snapping-scale-factor))]
      (for [part parts
            part (single-geometries (difference part holes))]
        part))))

(defn cut-line 
  "Cut a linestring or linear ring with a geometry. The geometry will be 
   linearized before use. The line will be cut at every point where the 
   cutter intersects the line.
   
   If any individual intersection of the cutter with the line is linear,
   the line will be cut at the start and end of the linear intersection.
   
   Returns a sequence of linear geometries."
  [line cutter]
  {:pre [(#{:line-string :linear-ring} (geometry-type line))]}
  (let [lil (new org.locationtech.jts.linearref.LengthIndexedLine line)
        cutter (->> cutter
                    (linearize)
                    (mapcat line-strings-of)
                    (make-multi-line-string))
        ;; find the points where the cutter intersects the line
        ;; if any intersects are linear, we take their start and end points
        intersect-points (->> (intersection line cutter)
                              (single-geometries)
                              (remove empty-geom?)
                              (mapcat #(case (geometry-type %)
                                         :point [%]
                                         :line-string (endpoints-of %))))
        ;; convert them to length-based indexes on the line
        intersect-indices (->> intersect-points
                               (map coordinate)
                               (mapv #(.indexOf lil %)))
        ;; if it's not a ring, add the start and end of the line
        intersect-indices (->> (if (= (geometry-type line) :linear-ring)
                                 intersect-indices
                                 (concat intersect-indices [(.getStartIndex lil) (.getEndIndex lil)]))
                               (set)
                               (sort))

        ;; for each pair of indexes, extract the connecting line:
        segments (->> intersect-indices
                      (map (fn [i1 i2] (.extractLine lil i1 i2))
                           (next intersect-indices)))]
    ;; for rings, we have to add the final segment that crosses the start and end index back in:
    (if (= (geometry-type line) :linear-ring)
      (if (empty? segments)
        [line]
        (conj segments (-> [(.extractLine lil (apply max intersect-indices) (.getEndIndex lil))
                            (.extractLine lil (.getStartIndex lil) (apply min intersect-indices))]
                           make-multi-line-string
                           line-merge
                           first)))
      (if (empty? segments)
        [line]
        segments))))

(defn normalize [g]
  (update-geometry g (.norm (geometry g))))

(defn thinness-ratio
  "4π(A/P²)"
  [g]
  (let [g (geometry g)]
    (* 4 Math/PI (/ (area g) (Math/pow (.getLength g) 2)))))

(defn densify 
  "Densify a geometry by inserting extra vertices along the line segments contained in 
   the geometry. All segments in the created densified geometry will be no longer than 
   `max-len`."
  [g max-len]
  (update-geometry g (Densifier/densify (geometry g) max-len)))

(defn interpolate 
  "return a new coord which is interpolated by `pct` % between `c1` and `c2`.
   "
  [^Coordinate c1 ^Coordinate c2 pct]
  (cond
    (= pct 0.0) c1
    (= pct 1.0) c2
    :else
    (make-coordinate (+ (.-x c1) (* (- (.-x c2) (.-x c1)) pct))
                     (+ (.-y c1) (* (- (.-y c2) (.-y c1)) pct)))))

(defn smooth 
  "Chaikin smoothing: replace each coordinate in the geometry with two new points, the first
   25% towards the previous point and the second 25% towards the next point. Repeat this process
   `iterations` times.

   Could be extended to handle linear rings, polygons, and multipart geometries.
  "
  [g iterations]
  {:pre [(= :line-string (geometry-type g))]}
  (if (empty-geom? g)
    g
    (loop [g g
           iterations iterations]
      (if (<= iterations 0)
        g
        (let [coords (make-coordinates g)
              
              new-coords (mapcat (fn [c1 c2 c3] [(interpolate c1 c2 0.75)
                                                 (interpolate c2 c3 0.25)])
                                 coords (next coords) (nnext coords))
              
              g (make-line-string `[~(first coords) ~@new-coords ~(last coords)])]
          (recur g (dec iterations)))))))

;; envelopes

(defn envelope-of ^org.locationtech.jts.geom.Envelope [g]
  (.getEnvelopeInternal (geometry g)))

(defn envelope->polygon [^org.locationtech.jts.geom.Envelope e]
  (normalize
   (make-polygon [[(.getMinX e) (.getMinY e)]
                  [(.getMaxX e) (.getMinY e)]
                  [(.getMaxX e) (.getMaxY e)]
                  [(.getMinX e) (.getMaxY e)]
                  [(.getMinX e) (.getMinY e)]])))
