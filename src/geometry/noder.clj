(ns geometry.noder
  (:require [geometry.core :as g]
            [geometry.index :as i]
            [taoensso.timbre :as log])
  (:import
   [org.locationtech.jts.geom Coordinate]
   [org.locationtech.jts.noding.snapround SnapRoundingNoder]
   [org.locationtech.jts.noding
    SegmentString NodedSegmentString 
    SegmentStringDissolver SegmentStringDissolver$SegmentStringMerger]))

(defn node
  "Given a set of linear features, node and dissolve to a linework
  Returns a seq of linestrings, whose userdata will be maps having the key ::lines
  which indicates the members of linear-features that went in."
  [linear-features
   & {:keys [get-meta merge-meta cm-precision]
      :or {get-meta   (fn [x] {::lines [x]})
           merge-meta (fn [xs ys] {::lines (into (::lines xs) (::lines ys))})
           cm-precision 10}}]
  (let [noder (SnapRoundingNoder. (g/fixed-precision-model cm-precision))
        
        _ (.computeNodes
           noder
           (for [f linear-features]
             (NodedSegmentString. (g/make-coordinates f) (get-meta f))))
        
        dissolver (SegmentStringDissolver.
                   (reify SegmentStringDissolver$SegmentStringMerger
                     (merge [_
                             target
                             source
                             _orientation]
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

(defn connect-faces
  "A function you can use with `node-with-others` to select nicer
  connecting lines.

  If `target` is a polygon then any segment of its boundary that is
  longer than `face-length` will have a point put in the middle. Then
  the connector will go to one of those points, unless the simple
  shortest line is more than `tolerance` shorter than the result.
  "
  [face-length tolerance line target]

  (let [straight (g/closest-points-on line target)

        d0 (g/distance (first straight) (second straight))

        to-face (->> (g/boundary-of target)
                     (g/make-coordinates)
                     (partition 2 1)
                     (keep (fn [[a b]]
                             (when (>= (.distance ^Coordinate a ^Coordinate b) face-length)
                               (let [[on-line pt]
                                     (g/closest-points-on
                                      line
                                      (g/make-point
                                       (/ (+ (.getX a) (.getX b)) 2.0)
                                       (/ (+ (.getY a) (.getY b)) 2.0)))

                                     d (g/distance on-line pt)]
                                 (when (<= (- d0 d) tolerance)
                                   [d [on-line pt]])))))
                     (sort-by first)
                     (first))]
    (or (second to-face) straight)))

(defn node-with-others
  "Returns [lines, {other feature -> point of connection}].
  Lines from the input set of lines have user-data with ::lines [input lines];
  some of these may be splits or joins of input lines.

  When connecting a feature to a line, the :connect argument will be called
  like (fn [line target-feature]) => [point-A point-B]
  where point-A is a point on the line and point-B is where to connect to the
  target feature. You can use this to make the connectors more fancy if you like.

  Its default value, `geometry.core/closest-points-on` just makes the shortest
  line possible.
  "
  [linear-features other-features & {:keys [cm-precision connect range]
                                     :or {cm-precision 10
                                          range 1000.0
                                          connect g/closest-points-on}}]
  
  (let [linear-features (node linear-features
                              :get-meta (fn [x] {::lines [x]})
                              :merge-meta (fn [x y] (merge-with into x y))
                              :cm-precision cm-precision)
        
        index (i/create linear-features)

        [linear-features target-points]
        (loop [linear-features (set linear-features)
               index           index
               other-features  other-features
               target-points   {}]
          (if (empty? other-features)
            [linear-features target-points]

            (let [[target & other-features] other-features
                  line  (first (i/neighbours index target range 5))]
              (if line
                (let [[line-point target-point] (connect line target)
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
                (recur linear-features index other-features target-points)))))]
    [linear-features target-points]))

