(ns geometry.noder
  (:require [geometry.core :as g]
            [geometry.index :as i]
            [taoensso.timbre :as log])
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
  "Returns [lines, {other feature -> point of connection}].
  Lines from the input set of lines have user-data with ::lines [input lines];
  some of these may be splits or joins of input lines.
  "
  [linear-features other-features]
  
  (let [linear-features (node linear-features
                              :get-meta (fn [x] {::lines [x]})
                              :merge-meta (fn [x y] (merge-with into x y)))
        
        index (i/create linear-features)

        [linear-features target-points]
        (loop [linear-features (set linear-features)
               index           index
               other-features  other-features
               target-points   {}
               ]
          (if (empty? other-features)
            [linear-features target-points]

            (let [[target & other-features] other-features
                  line  (first (i/neighbours index target 1000.0 5))]
              (if line
                (let [[line-point target-point] (g/closest-points-on line target)
                      [line-start line-end]     (g/endpoints-of line)
                      new-line                  (and (not= target-point line-point)
                                                     (g/make-line-string [line-point target-point]))
                      ]
                  (if (or (= line-start line-point) (= line-end line-point))
                    (recur (cond-> linear-features new-line (conj new-line))
                           index
                           other-features
                           (assoc target-points target target-point))
                    
                    (let [[line-a line-b] (g/split-line line line-point)]
                      (recur (-> linear-features
                                 (disj line)
                                 (conj line-a line-b)
                                 (cond-> new-line (conj new-line)))
                             (-> index
                                 (i/delete line)
                                 (i/add line-a)
                                 (i/add line-b)
                                 ;; (cond-> new-line (i/add new-line))
                                 )
                             other-features
                             (assoc target-points target target-point)))))
                (do (log/warn "No connecting line for %s, it will get lost" target)
                    (recur linear-features index other-features target-points))))))]
    [linear-features target-points]))

