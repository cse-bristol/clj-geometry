(ns geometry.grid
  (:require [geometry.core :as geometry]))

(defn make-grid
  "Make a grid of square polygons with height and width `dimension` covering
   the geometry or Feature `g`.

   Grid cells will be aligned with the minimum x and y coordinates of `g`'s envelope."
  [g dimension]
  (let [g (geometry/geometry g)
        envelope (doto (.getEnvelopeInternal g)
                   (.expandBy dimension))
        dimension (double dimension)
        min-x (.getMinX envelope)
        min-y (.getMinY envelope)
        max-x (.getMaxX envelope)
        max-y (.getMaxY envelope)
        coords (fn [x y]
                 (let [x+ (+ x dimension)
                       y+ (+ y dimension)]
                   [[x y] [x+ y] [x+ y+] [x y+] [x y]]))]
    (vec
     (for [x (range min-x max-x dimension)
           y (range min-y max-y dimension)]
       (geometry/make-polygon (coords x y))))))
