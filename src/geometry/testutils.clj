(ns geometry.testutils
  (:require [geometry.core :as geom]
            [geometry.feature :as f]))

(defmacro g=
  "Compare geometries or sequences of geometries for equality. If they are Features,
   their fields will be ignored."
  [& args]
  `(apply = (map (fn [g#] (if (sequential? g#)
                             (map #(geometry.core/normalize (geometry.core/geometry %)) g#)
                             (geometry.core/normalize (geometry.core/geometry g#))))
                 ~(vec args))))

(defmacro f=
  "Compare geometries/Features or sequences of geometries/Features for equality. 
   If they are Features, their fields will *not* be ignored."
  [& args]
  `(apply = (map (fn [g#] (if (sequential? g#)
                             (map geometry.core/normalize g#)
                             (geometry.core/normalize g#)))
                 ~(vec args))))

(defn circle
  "x, y: centre of circle
   r: radius"
  [x y r]
  (geom/buffer (geom/make-point x y) r))

(defn square
  "x, y: centre of square
   e: edge length"
  [x y e]
  (let [e (/ e 2)]
    (geom/make-polygon [[(- x e) (- y e)]
                        [(+ x e) (- y e)]
                        [(+ x e) (+ y e)]
                        [(- x e) (+ y e)]
                        [(- x e) (- y e)]])))

(defn square-feature
  "x, y: centre of square
   e: edge length.
   
   Makes a Feature rather than a geometry"
  ([x y e]
   (square-feature x y e {}))
  ([x y e attrs]
   (f/map->Feature (merge attrs {:geometry (square x y e)}))))

(defn circle-feature
  "x, y: centre of circle
   r: radius
   
   Makes a Feature rather than a geometry"
  ([x y e]
   (circle-feature x y e {}))
  ([x y r attrs]
   (f/map->Feature (merge attrs {:geometry (circle x y r)}))))