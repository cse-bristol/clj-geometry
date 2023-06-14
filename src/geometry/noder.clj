(ns geometry.noder
  (:require [geometry.core :as g]
            [geometry.index :as i])
  (:import
   [org.locationtech.jts.noding.snapround SnapRoundingNoder]
   [org.locationtech.jts.noding
    SegmentString MCIndexNoder NodedSegmentString IntersectionAdder
    SegmentStringDissolver SegmentStringDissolver$SegmentStringMerger]
   [org.locationtech.jts.geom Coordinate]
   [org.locationtech.jts.algorithm RobustLineIntersector]
   [org.locationtech.jts.geom Geometry Point Envelope PrecisionModel GeometryFactory Coordinate LineString]))

(defn node
  "Given a set of linear features, node and dissolve to a linework
  Returns a seq of linestrings, whose userdata will be maps having the key ::lines
  which indicates the members of linear-features that went in."
  [linear-features
   & {:keys [get-meta merge-meta precision-model]
      :or {get-meta   (fn [x] {::lines [x]})
           merge-meta (fn [xs ys] {::lines (into (::lines xs) (::lines ys))})
           precision-model (.getPrecisionModel g/*factory*)}}]
  (let [noder (SnapRoundingNoder. precision-model)
        
        _ (.computeNodes
           noder
           (for [f linear-features]
             (NodedSegmentString. (g/make-coordinates f) (get-meta f))))
        
        dissolver (SegmentStringDissolver.
                   (reify SegmentStringDissolver$SegmentStringMerger
                     (merge [_
                             target
                             source
                             orientation]
                       (.setData target (merge-meta (.getData target)
                                                    (.getData source))))))
        noded-segments (do
                         (.dissolve dissolver (.getNodedSubstrings noder))
                         (.getDissolved dissolver))
        ]
    (for [^SegmentString s noded-segments]
      (g/set-user-data!
       (g/make-line-string (.getCoordinates s))
       (.getData s)))))

(defn node-with-points
  "Returns a linework (collection of linestrings) for which userdata is a map
  having either ::lines or ::points depending on whether the linestring
  is made from dissolving linear features or connecting a point feature.
  This should be sufficient to build an adjacency matrix."
  [linear-features point-features]
  (let [index            (i/create linear-features)
        connecting-lines (for [point point-features
                               line  (i/neighbours index point 500.0)]
                           (let [point-on-line (g/closest-point-on line point)]
                             (g/set-user-data!
                              (g/make-line-string
                               [(g/geometry point) point-on-line])
                              {::points [point]})))
        ;; slightly ugly use of set-user-data! to pass provenance of lines
        ;; down below.
        network (node (concat linear-features connecting-lines)
                      :get-meta
                      (fn [x]
                        (let [u (g/user-data x)]
                          (if (::points u) u {::lines [x]})))
                      :merge-meta
                      (fn [xs ys] (merge-with into xs ys)))]
    network))
