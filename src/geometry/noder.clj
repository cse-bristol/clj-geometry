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

(defn node-with-others
  "Returns a linework (collection of linestrings) for which userdata is a map
  having either ::lines or ::points depending on whether the linestring
  is made from dissolving linear features or connecting a point feature.

  in theory an output linestring could maybe have all if it was something like

  *---L---*====P1====P2

  this would dissolve to one string that has ::others [P1 P2] ::lines [L]
  
  This should be sufficient to build an adjacency matrix; you might
  need to do a bit more geometry work to deal with the very rare case
  above from collinear points

  The resulting linework may not form a connected graph, mind"
  [linear-features other-features]
  (let [index            (i/create linear-features)
        connecting-lines (for [target other-features
                               ;; TODO take n neighbours?
                               line  (i/neighbours index target 500.0 1)]
                           (let [[lp tp]
                                 (g/closest-points-on line target)]
                             (g/set-user-data!
                              (g/make-line-string [lp tp])
                              {::others [target]})))
        ;; slightly ugly use of set-user-data! to pass provenance of lines
        ;; down below.
        network (node (concat linear-features connecting-lines)
                      :get-meta
                      (fn [x]
                        (let [u (g/user-data x)]
                          (if (::others u) u {::lines [x]})))
                      :merge-meta
                      (fn [xs ys] (merge-with into xs ys)))]
    network))
