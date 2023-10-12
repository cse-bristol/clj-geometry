(ns geometry.util
  "Utility functions for operating on geometries"
  (:require [geometry.core :as g]
            [taoensso.timbre :as log]
            )
  (:import [org.locationtech.jts.noding SegmentStringDissolver SegmentStringUtil]
           [org.locationtech.jts.geom Geometry PrecisionModel]
           [org.locationtech.jts.noding.snapround SnapRoundingNoder]
           [org.locationtech.jts.geom GeometryFactory]
           [org.locationtech.jts.operation.polygonize Polygonizer]
           )
  )

(def ^:dynamic *factory*
  (let [csf

        ^org.locationtech.jts.geom.CoordinateSequenceFactory
        (org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory/instance)

        ;; for some reason, reading from geopackage constructs
        ;; CoordinateXY nowadays, which break when used with some SRS.
        ;; so we make a special factory which can never produce such
        ;; coordinates.
        csf-hack
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

            (.create csf size (int 3) measures)))]
    (GeometryFactory. csf-hack)))


(defn node-paths
  "From JTS docs: To specify 3 decimal places of precision,
   use a scale factor of 1000.
   To specify -3 decimal places of precision (i.e. rounding to
   the nearest 1000), use a scale factor of 0.001."
  [paths & {:keys [snapping-scale-factor]
            :or {snapping-scale-factor 10.0}}]
  (let [paths (->> paths
                   (map (fn [^Geometry g] (:geometry g))))
        pm (new PrecisionModel (float snapping-scale-factor))
        noder (new SnapRoundingNoder pm)
        noded-paths (mapcat #(SegmentStringUtil/extractSegmentStrings %) paths)
        noded-paths (do (.computeNodes noder noded-paths)
                        (.getNodedSubstrings noder))
        dissolver (new SegmentStringDissolver)
        noded-paths (do (.dissolve dissolver noded-paths)
                        (.getDissolved dissolver))]
    (SegmentStringUtil/toGeometry noded-paths *factory*)))


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
        invalid (remove g/valid? polygons)]
    (log/infof "polygonized paths: %d polygons - %d invalid ring lines - %d dangles - %d cut-edges",
               (count polygons) invalid-ring-lines dangles cut-edges)
    (log/infof "%d invalid polygons" (count invalid))
    polygons))
