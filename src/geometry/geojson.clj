(ns geometry.geojson
  (:require [clojure.data.json :as json]
            [geometry.core :as g]
            [geometry.feature :as f]))

(defn- map->geom
  "Convert a geojson geometry map into a jts geometry directly"
  [m]

  (let [coordinates (or (:coordinates m)
                        (get m "coordinates"))
        type (or (:type m)
                 (get m "type"))]
    (case type
      "Point"
      (g/make-point coordinates)
      "LineString"
      (g/make-line-string coordinates)
      "Polygon"
      (g/make-polygon (first coordinates)
                      (rest coordinates))
      "MultiPoint"
      (g/make-multi-point
       (for [cs coordinates]
         (g/make-point cs)))

      "MultiLineString"
      (g/make-multi-line-string
       (for [cs coordinates]
         (g/make-line-string cs)))

      "MultiPolygon"
      (g/make-multi-polygon
       (for [cs coordinates]
         (g/make-polygon (first cs) (rest cs))))

      "GeometryCollection"
      (g/make-collection
       (for [g (or (:geometries m) (get m "geometries"))]
         (map->geom g)))

      (throw (ex-info "Unknown geometry-type" {:geometry-type type})))))

(defn open [reader & {:keys [key-transform]
                      :or   {key-transform identity}}]
  (let [obj (json/read reader)
        properties (fn [p]
                     (into {} (for [[k v] p] [(key-transform k) v])))]
    (case (get obj "type")
      "FeatureCollection"
      (->> (get obj "features")
           (mapv #(f/map->Feature
                   (assoc (properties (get % "properties"))
                          :geometry (map->geom (get % "geometry"))))))

      "Feature"
      [(f/map->Feature
        (assoc (properties (get obj "properties"))
               :geometry (map->geom (get obj "geometry"))))]

      [(f/map->Feature
        {:geometry (map->geom (get obj "geometry"))})])))
