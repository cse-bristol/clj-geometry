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

(defn- do-snap-endpoints
  "Snap the endpoints only of linear-features. This doesn't snap to a grid
  Instead it moves any pair of endpoints that are this close to one another onto
  one of the pair, in arbitrary order.

  This may filter out features if they get collapsed to a single point."
  [linear-features cm-precision]
  (loop [inputs linear-features
         outputs nil
         index i/EMPTY]
    (if (seq inputs)
      (let [this-feature (first inputs)
            [a b] (g/endpoints-of this-feature)
            ai (first (i/query index a (/ cm-precision 100.0)))
            bi (first (i/query index b (/ cm-precision 100.0)))]
        (if (or (and ai (not= ai a)) (and bi (not= bi b)))
          (let [c (g/make-coordinates this-feature)
                _ (do (aset c 0 (g/coordinate (or ai a)))
                      (aset c (dec (count c)) (g/coordinate (or bi b))))
                g2 (g/make-valid (g/make-line-string c))]
            (recur (rest inputs)
                   (cond-> outputs
                     (and (g/valid? g2)
                          (not (g/empty-geom? g2))
                          (g/line-string? g2))
                     (conj (g/update-geometry this-feature g2)))
                   (-> index (i/add (or ai a)) (i/add (or bi b)))))
          (recur (rest inputs)
                 (conj outputs this-feature)
                 (-> index (i/add a) (i/add b)))))
      outputs)))

(defn node
  "Given a set of linear features, node and dissolve to a linework
  Returns a seq of linestrings, whose userdata will be maps having the key ::lines
  which indicates the members of linear-features that went in.

  named args are
  `:get-meta` and `:merge-meta`; used to transfer metadata onto the new linestrings
  `:cm-precision`; precision in cm (assuming CRS is in meters) for noding
  `:snap-endpoints` if true, endpoints of all lines are snapped first.
    This prevents tiny overlaps and closes tiny holes.

  returns a set of linestrings, which have user-data per :merge-meta.
  "
  [linear-features
   & {:keys [get-meta merge-meta cm-precision snap-endpoints]
      :or {get-meta   (fn [x] {::lines [x]})
           merge-meta (fn [xs ys] {::lines (into (::lines xs) (::lines ys))})
           cm-precision 10
           snap-endpoints true}}]
  (let [noder (SnapRoundingNoder. (g/fixed-precision-model cm-precision))

        linear-features (cond-> linear-features
                          snap-endpoints
                          (do-snap-endpoints cm-precision))
        
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

