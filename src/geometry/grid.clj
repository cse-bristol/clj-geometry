(ns geometry.grid
  (:require [geometry.core :as geometry])
  (:import [org.geotools.feature FeatureIterator]))

(defn make-grid
  "Make a grid of square polygons with height and width `dimension` covering
   the geometry or Feature `g`. 
   
   Grid cells will be aligned with the minimum x and y coordinates of `g`'s envelope."
  [g dimension]
  (let [g (geometry/geometry g)
        envelope (doto (.getEnvelopeInternal g)
                   (.expandBy dimension))

        grid (org.geotools.grid.Grids/createSquareGrid
              (org.geotools.geometry.jts.ReferencedEnvelope. envelope nil)
              dimension)

        feature-iterator-seq
        (fn feature-iterator-seq [^FeatureIterator feature-iterator]
          (lazy-seq
           (if (.hasNext feature-iterator)
             (cons (.next feature-iterator)
                   (feature-iterator-seq feature-iterator))
             (do (.close feature-iterator)
                 nil))))]
    (->> grid
         (.getFeatures)
         (.features)
         (feature-iterator-seq)
         (mapv #(.getDefaultGeometry ^org.opengis.feature.simple.SimpleFeature %)))))

