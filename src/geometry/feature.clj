(ns geometry.feature
  "A feature is a thing with geometry and attributes"
  (:require [geometry.core :as g])
  (:import [org.locationtech.jts.geom Geometry]))

(defrecord Feature [^Geometry geometry table crs]
  g/HasGeometry
  (geometry [f] (:geometry f))
  (update-geometry [f g] (assoc f :geometry g)))

(defn table-name [^Feature f] (.table f))

(defn crs [^Feature f] (.crs f))
