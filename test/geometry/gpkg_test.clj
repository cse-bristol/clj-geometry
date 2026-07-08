(ns geometry.gpkg-test
  (:require [geometry.gpkg :as sut]
            [geometry.gpkg.encode :as enc]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [geometry.core :as g]
            [geometry.feature :as f]
            [next.jdbc :as jdbc])
  (:import [clojure.lang IReduceInit]))

(defmacro with-temp-file [file & body]
  `(let [~file (.toFile (java.nio.file.Files/createTempFile
                         "test-read-write" ".gpkg"
                         (into-array java.nio.file.attribute.FileAttribute [])))]
     (try ~@body
          (catch Exception e# (prn e#) (throw e#))
          (finally (io/delete-file ~file true)))))


(t/deftest test-spatial-roundtrip
  (t/testing "writing to a table twice in a row appends"
    (with-temp-file f
      ;; write some features down
      (sut/write
       f
       "test-table"

       [{"geometry" (g/make-point 1 2) "id" 1 "b" "abc" :c true :inf ##Inf}
        {"geometry" (g/make-point 4 5) "id" 2 "b" "def" :c false :inf ##Inf}
        {"geometry" nil "id" 3 "b" "ghi" :c false :inf ##Inf}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}
        "b" {:type :string}
        "c" {:type :boolean :accessor :c}
        "inf" {:type :double :accessor :inf}})

      ;; read them back and compare

      (with-open [in (sut/open f :table-name "test-table")]
        (t/is (= (set [{:geometry nil "id" 3 :table "test-table" :crs "EPSG:27700" "b" "ghi" "c" false "inf" ##Inf}
                       {:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700" "b" "abc" "c" true "inf" ##Inf}
                       {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700" "b" "def" "c" false "inf" ##Inf}])

                 ;; we map into {} to strip off the feature type
                 ;; since we want to do a simple comparison here
                 (set (map #(into {} %) (sut/features in)))))))))

(t/deftest test-to-crs-reprojection
  (with-temp-file f
    ;; an Ordnance Survey test point in British National Grid (EPSG:27700)
    (sut/write
     f "test-table"
     [{"geometry" (g/make-point 651409.903 313177.270) "id" 1}]
     :schema
     {"geometry" {:type :point :srid 27700}
      "id" {:type :integer}})

    (t/testing "reading with :to-crs reprojects geometries to WGS84"
      (with-open [in (sut/open f :table-name "test-table" :to-crs 4326)]
        (let [pt (:geometry (first (sut/features in)))]
          ;; proj4j uses a Helmert transform so allow a loose tolerance
          (t/is (< (Math/abs (- 1.7179 (.getX pt))) 0.01))
          (t/is (< (Math/abs (- 52.6576 (.getY pt))) 0.01))
          (t/is (= 4326 (.getSRID pt))))))

    (t/testing "without :to-crs the geometry is unchanged"
      (with-open [in (sut/open f :table-name "test-table")]
        (let [pt (:geometry (first (sut/features in)))]
          (t/is (= 651409.903 (.getX pt)))
          (t/is (= 27700 (.getSRID pt))))))))

(t/deftest double-write-test
  (with-temp-file f
    (t/testing "Writing to same table twice appends"
      (sut/write
       f "test-table"
       [{"geometry" (g/make-point 1 2) "id" 1}
        {"geometry" (g/make-point 4 5) "id" 2}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}})

      (sut/write
       f "test-table"
       [{"geometry" (g/make-point 1 2) "id" 3}
        {"geometry" (g/make-point 4 5) "id" 4}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}})

      (with-open [in (sut/open f :table-name "test-table")]
        (let [contents (sut/features in)]
          (t/is (= [{:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700"}
                    {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700"}
                    {:geometry (g/make-point 1 2) "id" 3 :table "test-table" :crs "EPSG:27700"}
                    {:geometry (g/make-point 4 5) "id" 4 :table "test-table" :crs "EPSG:27700"}]
                   (vec (map #(into {} %) contents)))))))

    (t/testing "Writing to same table twice doesn't append if you ask it not to"
      (sut/write
       f "test-table"
       [{"geometry" (g/make-point 1 2) "id" 1}
        {"geometry" (g/make-point 4 5) "id" 2}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}})

      (sut/write
       f "test-table"
       [{"geometry" (g/make-point 1 2) "id" 3}
        {"geometry" (g/make-point 4 5) "id" 4}]

       :if-exists :drop-table
       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}})

      (with-open [in (sut/open f :table-name "test-table")]
        (let [contents (sut/features in)]
          (t/is (= [{:geometry (g/make-point 1 2) "id" 3 :table "test-table" :crs "EPSG:27700"}
                    {:geometry (g/make-point 4 5) "id" 4 :table "test-table" :crs "EPSG:27700"}]
                   (vec (map #(into {} %) contents)))))))))

(t/deftest test-can-read-twice
  (with-temp-file f
    (sut/write
     f "test-table"
     [{"geometry" (g/make-point 1 2) "id" 1}
      {"geometry" (g/make-point 4 5) "id" 2}]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id" {:type :integer}})

    (with-open [in (sut/open f :table-name "test-table")]
      (let [ls (sut/features in)]
        (t/is (= [{:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700"}
                  {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700"}]
                 (vec (map #(into {} %) ls))))
        (t/is (= [{:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700"}
                  {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700"}]
                 (vec (map #(into {} %) ls))))))))

(t/deftest test-non-spatial-roundtrip
  (with-temp-file f
    ;; write some non-spatial data:
    (sut/write
     f
     "sqlite-table"

     [{:a 1 "b" "aaa" :c true :inf ##Inf}
      {:a 2 "b" "bbb" :c false :inf ##Inf}]

     :schema
     {"a" {:type Integer :accessor :a}
      "b" {:type String}
      "c" {:type Boolean :accessor :c}
      "inf" {:type :double :accessor :inf}})

    ;; read the non-spatial data:
    (with-open [in (sut/open f :table-name "sqlite-table")]
      (t/is (= [{:geometry nil "a" 1 "b" "aaa" "c" true "inf" ##Inf :table "sqlite-table" :crs nil}
                {:geometry nil "a" 2 "b" "bbb" "c" false "inf" ##Inf :table "sqlite-table" :crs nil}]
               (vec (map #(into {} %) (sut/features in))))))))

(t/deftest test-mixed-roundtrip
  (with-temp-file f
    ;; write some features down
    (sut/write
     f
     "test-table"

     [{"geometry" (g/make-point 1 2) "id" 1}
      {"geometry" (g/make-point 4 5) "id" 2}]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id" {:type Integer}})

    ;; write some non-spatial data:
    (sut/write
     f
     "data-table"

     [{:a 1 "b" "aaa" :c true :inf ##Inf}
      {:a 2 "b" "bbb" :c false :inf ##Inf}]

     :schema
     {"a" {:type Integer :accessor :a}
      "b" {:type String}
      "c" {:type Boolean :accessor :c}
      "inf" {:type :double :accessor :inf}})

    (with-open [in (sut/open f :spatial-only? false :key-transform keyword)]
      (t/is (= [{:geometry (g/make-point 1 2) :id 1 :table "test-table" :crs "EPSG:27700"}
                {:geometry (g/make-point 4 5) :id 2 :table "test-table" :crs "EPSG:27700"}
                {:geometry nil :a 1 :b "aaa" :c true :inf ##Inf :table "data-table" :crs nil}
                {:geometry nil :a 2 :b "bbb" :c false :inf ##Inf :table "data-table" :crs nil}]
               (vec (map #(into {} %) (sut/features in))))))))

(t/deftest test-empty-read-writes
  (with-temp-file f
    ;; write some features down
    (sut/write f "test-table" []
               :schema
               {"geometry" {:type :point :srid 27700}
                "id" {:type Integer}})

    ;; write some non-spatial data:
    (sut/write f "data-table" []
               :schema
               {"a" {:type Integer :accessor :a}
                "b" {:type String}
                "c" {:type Boolean :accessor :c}
                "inf" {:type :double :accessor :inf}})

    (with-open [in (sut/open f :spatial-only? false :key-transform keyword)]
      (t/is (= []
               (vec (map #(into {} %) (sut/features in))))))))

(t/deftest test-table-names
  (with-temp-file f
    (sut/write
     f
     "test-table"
     [{"geometry" (g/make-point 1 2) "id" 1}]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id" {:type Integer}})

    (sut/write
     f
     "data-table"
     [{:a 2 "b" "bbb" :c false :inf ##Inf}]

     :schema
     {"a" {:type Integer :accessor :a}
      "b" {:type String}
      "c" {:type Boolean :accessor :c}
      "inf" {:type :double :accessor :inf}})

    (t/is (= #{"test-table" "data-table"}
             (sut/table-names f)))

    (t/is (= #{"test-table"}
             (sut/table-names f :spatial-only? true)))))

(t/deftest test-empty-table
  (with-temp-file f
    (sut/write
     f "zempty" nil :schema {"bork" {:type :integer}})

    (sut/write
     f "full" [{:bork 1} {:bork 2} {:bork 9}] :schema {"bork" {:type :integer :accessor :bork}})

    (sut/write
     f "aempty" [] :schema {"bork" {:type :integer}})

    (let [in (with-open [g (sut/open f :key-transform keyword)] (doall (sut/features g)))]
      (t/is (= #{["full" 1] ["full" 2] ["full" 9]}
               (set (map (juxt :table :bork) in)))))))

(t/deftest test-releases-head
  (with-temp-file out-file
    (let [n (atom 0)

          ref (atom nil)
          _ (println "Constructing seq")

          query-seq (iterator-seq
                     (reify
                       java.util.Iterator
                       (next [_]
                         (println "Generating" (swap! n inc))
                         {:geometry (g/make-point [@n @n])
                          :long-string (str (repeat 100000 @n))})

                       (hasNext [_]
                         (let [gcd (and @ref (nil? (.get @ref)))]
                           ;; generally we make ~32 rows because the
                           ;; iterator is chunked so it will ask for the
                           ;; next 32 items as soon as we want the first
                           ;; and we only get gced once the call to
                           ;; write has got so far.
                           (when (> @n 30) (System/gc))
                           (let [result (and (< @n 100) (not gcd))]
                             result)))))]
      (reset! ref (java.lang.ref.WeakReference. query-seq))
      (println "Calling write...")
      (sut/write out-file "test"
                 query-seq
                 :schema
                 [["geometry" {:type :point :srid 27700 :accessor :geometry}]
                  ["long-string" {:type :string :accessor :long-string}]]
                 :batch-insert-size 1)
      (t/testing "The sequence head got garbage collected after we started writing"
        (t/is (< @n 90))))))


(t/deftest test-set-layer-extent
  (with-temp-file f
    ;; Points layer
    (sut/write
     f "test-table-points"
     [{"geometry" (g/make-point 1 2) "id" 1}
      {"geometry" (g/make-point 4 5) "id" 2}
      {"geometry" nil "id" 3}]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id" {:type :integer}}

     :add-spatial-index true)

    (with-open [in (sut/open f :table-name "gpkg_contents")]
      (let [ls (sut/features in)
            row (first ls)]
        (t/is (= 1.0 (get row "min_x")))
        (t/is (= 2.0 (get row "min_y")))
        (t/is (= 4.0 (get row "max_x")))
        (t/is (= 5.0 (get row "max_y")))))

    ;; Polygon layer
    (sut/write
     f "test-table-polygons"
     [{"geometry" (g/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]]) "id" 1}
      {"geometry" (g/make-polygon [[1 1] [1 2] [3 2] [3 1] [1 1]]) "id" 2}]

     :schema
     {"geometry" {:type :polygon :srid 27700}
      "id" {:type :integer}})

    (with-open [in (sut/open f :table-name "gpkg_contents")]
      (let [ls (sut/features in)
            row (second ls)]
        (t/is (= 0.0 (get row "min_x")))
        (t/is (= 0.0 (get row "min_y")))
        (t/is (= 3.0 (get row "max_x")))
        (t/is (= 2.0 (get row "max_y")))))

    ;; LineString layer
    (sut/write
     f "test-table"
     [{"geometry" (g/make-line-string [[0 0] [1 1]]) "id" 1}
      {"geometry" (g/make-line-string [[1 1] [1 3]]) "id" 2}]

     :schema
     {"geometry" {:type :line-string :srid 27700}
      "id" {:type :integer}})

    (with-open [in (sut/open f :table-name "gpkg_contents")]
      (let [ls (sut/features in)
            row (nth ls 2)]
        (t/is (= 0.0 (get row "min_x")))
        (t/is (= 0.0 (get row "min_y")))
        (t/is (= 1.0 (get row "max_x")))
        (t/is (= 3.0 (get row "max_y")))))))

(t/deftest test-escape-identifier
  (let [escape-identifier #'sut/escape-identifier]
    (t/is (= (escape-identifier "foo") "\"foo\""))
    (t/is (= (escape-identifier "foo.bar") "\"foo.bar\""))
    (t/is (= (escape-identifier "\"weird identifier.strange") "\"\"\"weird identifier.strange\""))
    (t/is (= (escape-identifier "foo" "bar") "\"foo\".\"bar\""))))

(t/deftest test-amend-gpkg
  (with-temp-file f
    ;; write some features down
    (sut/write
     f
     "test-table"

     [{"geometry" (g/make-point 1 2) "id" 1 "b" "abc" :c true :inf ##Inf}
      {"geometry" (g/make-point 4 5) "id" 2 "b" "def" :c false :inf ##Inf}
      {"geometry" nil "id" 3 "b" "ghi" :c false :inf ##Inf}]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id"       {:type :integer}
      "b"        {:type :string}
      "c"        {:type :boolean :accessor :c}
      "inf"      {:type :double :accessor :inf}})

    ;; read them back and compare

    (sut/amend
     f "test-table"
     (with-open [in (sut/open f :table-name "test-table" :rowids? true)]
       (mapv
        (fn [feature]
          (-> feature
              (g/update-geometry (g/make-point 3 3))
              (update "b" #(.toUpperCase %))
              (update "c" not)))
        (sut/features in)))
     :schema
     ;; update these fields only
     {"geometry" {:type :point :srid 27700 :accessor g/geometry}
      "b"        {:type :string}
      "c"        {:type :boolean}})

    ;; read back and check
    (t/is
     (= (group-by
         #(get % "id")
         [{:geometry (g/make-point 3 3) "id" 3 :table "test-table" :crs "EPSG:27700" "b" "GHI" "c" true "inf" ##Inf}
          {:geometry (g/make-point 3 3) "id" 1 :table "test-table" :crs "EPSG:27700" "b" "ABC" "c" false "inf" ##Inf}
          {:geometry (g/make-point 3 3) "id" 2 :table "test-table" :crs "EPSG:27700" "b" "DEF" "c" true "inf" ##Inf}])

        (with-open [in (sut/open f :table-name "test-table")]
          (group-by #(get % "id")
                    (map #(into {} %) (sut/features in)))))))
  
  (with-temp-file f
    ;; write some features down
    (sut/write
     f
     "test-table"

     [{"id" 1 "b" "abc" :c true :inf ##Inf}
      {"id" 2 "b" "def" :c false :inf ##Inf}
      {"id" 3 "b" "ghi" :c false :inf ##Inf}]

     :schema
     {"id"  {:type :integer}
      "b"   {:type :string}
      "c"   {:type :boolean :accessor :c}
      "inf" {:type :double :accessor :inf}})

    ;; read them back and compare

    (sut/amend
     f "test-table"
     (with-open [in (sut/open f :table-name "test-table" :rowids? true)]
       (mapv
        (fn [feature]
          (-> feature
              (update "b" #(.toUpperCase %))
              (update "c" not)))
        (sut/features in)))
     
     :schema {"b" {:type :string} "c" {:type :boolean}})

    ;; read back and check
    (t/is
     (= (group-by
         #(get % "id")
         [{"id" 3 :table "test-table" "b" "GHI" "c" true "inf" ##Inf}
          {"id" 1 :table "test-table" "b" "ABC" "c" false "inf" ##Inf}
          {"id" 2 :table "test-table" "b" "DEF" "c" true "inf" ##Inf}])

        (with-open [in (sut/open f :table-name "test-table")]
          (group-by #(get % "id")
                    (map #(dissoc (into {} %)
                                  :geometry :crs) (sut/features in))))))))

(t/deftest test-amend-left-join
  (with-temp-file f
    ;; write some features down
    (sut/write
     f
     "test-table"

     [{"geometry" (g/make-point 1 2) "id" 1 "b" "abc" :c true} ;; should get duplicated and c flipped
      {"geometry" (g/make-point 1 3) "id" 2 "b" "abc" :c true} ;; should get changed
      {"geometry" (g/make-point 1 4) "id" 3 "b" "qwe" :c true} ;; should be untouched
      ]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id"       {:type :integer}
      "b"        {:type :string}
      "c"        {:type :boolean :accessor :c}})

    ;; read them back and compare

    (sut/amend
     f "test-table"
     (with-open [in (sut/open f :table-name "test-table" :rowids? true)]
       (mapcat
        (fn [feature]
          (case (get feature "id")
            1 [(-> feature
                   (g/update-geometry (g/make-point 3 3))
                   (update "b" #(str "ONE " (.toUpperCase %)))
                   (update "c" not))
               
               (-> feature
                   (g/update-geometry (g/make-point 4 4))
                   (update "b" #(str "TWO " (.toUpperCase %)))
                   (update "c" not))
               ]

            2 [(assoc feature "b" "ONLY")]

            [] ;; nop
            ))
        (sut/features in)))
     :if-exists :preserve
     :method :left-join
     :schema
     ;; update these fields only
     {"geometry" {:type :point :srid 27700 :accessor g/geometry}
      "b"        {:type :string}
      "c"        {:type :boolean}})

    ;; read back and check
    (t/is
     (= (group-by
         #(get % "id")
         [{:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 3 3) "id" 1 "b" "ONE ABC" "c" false}
          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 4 4) "id" 1 "b" "TWO ABC" "c" false}
          
          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 1 3) "id" 2 "b" "ONLY" "c" true}
          
          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 1 4) "id" 3 "b" "qwe" "c" true}

          ])

        (with-open [in (sut/open f :table-name "test-table")]
          (group-by #(get % "id")
                    (map #(into {} %) (sut/features in))))))))


(t/deftest test-amend-outer-join
  (with-temp-file f
    ;; write some features down
    (sut/write
     f
     "test-table"

     [{"geometry" (g/make-point 1 2) "id" 1 "b" "abc" :c true} ;; should get duplicated
      {"geometry" (g/make-point 1 3) "id" 2 "b" "abc" :c true} ;; should get changed
      {"geometry" (g/make-point 1 4) "id" 3 "b" "qwe" :c true} ;; should be untouched
      ]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id"       {:type :integer}
      "b"        {:type :string}
      "c"        {:type :boolean :accessor :c}})

    ;; read them back and compare

    (sut/amend
     f "test-table"
     (with-open [in (sut/open f :table-name "test-table" :rowids? true)]
       (concat
        [(f/map->Feature
          {:geometry (g/make-point 99 99)
           :srid 27700
           "b" "NEW"
           "c" false
           })]
        (mapcat
         (fn [feature]
           (case (get feature "id")
             1 [(-> feature
                    (g/update-geometry (g/make-point 3 3))
                    (update "b" #(str "ONE " (.toUpperCase %)))
                    (update "c" not))
                
                (-> feature
                    (g/update-geometry (g/make-point 4 4))
                    (update "b" #(str "TWO " (.toUpperCase %)))
                    (update "c" not))
                ]

             2 [(assoc feature "b" "ONLY")]

             [] ;; nop
             ))
         (sut/features in))))
     :if-exists :preserve
     :method :outer-join
     :schema
     ;; update these fields only
     {"geometry" {:type :point :srid 27700 :accessor g/geometry}
      "b"        {:type :string}
      "c"        {:type :boolean}})

    ;; read back and check
    (t/is
     (= (group-by
         #(get % "id")
         [{:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 3 3) "id" 1 "b" "ONE ABC" "c" false}
          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 4 4) "id" 1 "b" "TWO ABC" "c" false}
          
          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 1 3) "id" 2 "b" "ONLY" "c" true}
          
          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 1 4) "id" 3 "b" "qwe" "c" true}

          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 99 99) "id" nil "b" "NEW" "c" false}

          ])

        (with-open [in (sut/open f :table-name "test-table")]
          (group-by #(get % "id")
                    (map #(into {} %) (sut/features in))))))))

(defn- query [f sql]
  (with-open [conn (jdbc/get-connection
                    (jdbc/get-datasource (str "jdbc:sqlite:" (.getCanonicalPath f))))]
    (jdbc/execute! conn [sql])))

(t/deftest test-gpkg-headers
  (with-temp-file f
    (sut/write
     f "test-table"
     [{"geometry" (g/make-point 1 2) "id" 1}]
     :schema {"geometry" {:type :point :srid 27700} "id" {:type :integer}})

    (t/testing "application_id and user_version identify the file as a GeoPackage"
      ;; 1196444487 = 0x47504B47 = \"GPKG\"; 10200 = GeoPackage 1.2
      (t/is (= 1196444487 (:application_id (first (query f "PRAGMA application_id")))))
      (t/is (= 10200 (:user_version (first (query f "PRAGMA user_version"))))))))

(t/deftest test-spatial-index-populated
  (with-temp-file f
    (sut/write
     f "test-table"
     [{"geometry" (g/make-point 10 20) "id" 1}
      {"geometry" (g/make-point 30 40) "id" 2}
      {"geometry" nil "id" 3}] ;; null geometry must NOT be indexed
     :schema {"geometry" {:type :point :srid 27700} "id" {:type :integer}}
     :add-spatial-index true)

    (t/testing "the rtree extension is registered"
      (t/is (= [{:gpkg_extensions/table_name "test-table"
                 :gpkg_extensions/extension_name "gpkg_rtree_index"}]
               (query f "SELECT table_name, extension_name FROM gpkg_extensions"))))

    (t/testing "the rtree index is populated by the triggers, excluding null geometries"
      (t/is (= #{[1 10.0 10.0 20.0 20.0]
                 [2 30.0 30.0 40.0 40.0]}
               (set (map (juxt :rtree_test-table_geometry/id
                               :rtree_test-table_geometry/minx
                               :rtree_test-table_geometry/maxx
                               :rtree_test-table_geometry/miny
                               :rtree_test-table_geometry/maxy)
                         (query f "SELECT id, minx, maxx, miny, maxy FROM \"rtree_test-table_geometry\""))))))

    (t/testing "an update to a geometry is reflected in the index by the triggers"
      (with-open [conn (#'sut/open-sqlite f)]
        (jdbc/execute! conn
                       ["UPDATE \"test-table\" SET geometry = ? WHERE id = 1"
                        (#'geometry.gpkg.encode/encode (g/make-point 100 200) 27700)]))
      (t/is (= #{[1 100.0 100.0 200.0 200.0]
                 [2 30.0 30.0 40.0 40.0]}
               (set (map (juxt :rtree_test-table_geometry/id
                               :rtree_test-table_geometry/minx
                               :rtree_test-table_geometry/maxx
                               :rtree_test-table_geometry/miny
                               :rtree_test-table_geometry/maxy)
                         (query f "SELECT id, minx, maxx, miny, maxy FROM \"rtree_test-table_geometry\""))))))))

(t/deftest test-drop-table-cleanup
  (with-temp-file f
    (sut/write
     f "test-table"
     [{"geometry" (g/make-point 1 2) "id" 1}]
     :schema {"geometry" {:type :point :srid 27700} "id" {:type :integer}}
     :add-spatial-index true)

    ;; sanity: everything is present before the drop
    (t/is (contains? (sut/table-names f) "test-table"))
    (t/is (seq (query f "SELECT name FROM sqlite_master WHERE name = 'rtree_test-table_geometry'")))

    (sut/drop-table f "test-table")

    (t/testing "the table itself and its rtree virtual table are gone"
      (t/is (not (contains? (sut/table-names f) "test-table")))
      (t/is (empty? (query f "SELECT name FROM sqlite_master WHERE name = 'rtree_test-table_geometry'"))))

    (t/testing "the rtree triggers are gone"
      (t/is (empty? (query f "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'rtree_test-table_geometry%'"))))

    (t/testing "GeoPackage metadata rows are cleaned up"
      (t/is (empty? (query f "SELECT table_name FROM gpkg_contents WHERE table_name = 'test-table'")))
      (t/is (empty? (query f "SELECT table_name FROM gpkg_geometry_columns WHERE table_name = 'test-table'")))
      (t/is (empty? (query f "SELECT table_name FROM gpkg_extensions WHERE table_name = 'test-table'"))))))

(t/deftest test-amend-right-join
  (with-temp-file f
    ;; write some features down
    (sut/write
     f
     "test-table"

     [{"geometry" (g/make-point 1 2) "id" 1 "b" "abc" :c true} ;; matched, gets duplicated
      {"geometry" (g/make-point 1 3) "id" 2 "b" "abc" :c true} ;; matched, gets changed
      {"geometry" (g/make-point 1 4) "id" 3 "b" "qwe" :c true} ;; unmatched -> deleted by right-join
      ]

     :schema
     {"geometry" {:type :point :srid 27700}
      "id"       {:type :integer}
      "b"        {:type :string}
      "c"        {:type :boolean :accessor :c}})

    (sut/amend
     f "test-table"
     (with-open [in (sut/open f :table-name "test-table" :rowids? true)]
       (concat
        ;; a brand new row, unmatched in the target -> inserted
        [(f/map->Feature
          {:geometry (g/make-point 99 99)
           :srid 27700
           "b" "NEW"
           "c" false})]
        (mapcat
         (fn [feature]
           (case (get feature "id")
             1 [(-> feature
                    (g/update-geometry (g/make-point 3 3))
                    (update "b" #(str "ONE " (.toUpperCase %)))
                    (update "c" not))
                (-> feature
                    (g/update-geometry (g/make-point 4 4))
                    (update "b" #(str "TWO " (.toUpperCase %)))
                    (update "c" not))]

             2 [(assoc feature "b" "ONLY")]

             [] ;; id 3 omitted: unmatched in update -> deleted
             ))
         (sut/features in))))
     :if-exists :preserve
     :method :right-join
     :schema
     {"geometry" {:type :point :srid 27700 :accessor g/geometry}
      "b"        {:type :string}
      "c"        {:type :boolean}})

    ;; read back and check: id 3 is gone, the new row is present
    (t/is
     (= (group-by
         #(get % "id")
         [{:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 3 3) "id" 1 "b" "ONE ABC" "c" false}
          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 4 4) "id" 1 "b" "TWO ABC" "c" false}

          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 1 3) "id" 2 "b" "ONLY" "c" true}

          {:crs "EPSG:27700" :table "test-table" :geometry (g/make-point 99 99) "id" nil "b" "NEW" "c" false}])

        (with-open [in (sut/open f :table-name "test-table")]
          (group-by #(get % "id")
                    (map #(into {} %) (sut/features in))))))))

(t/deftest test-schema-extension
  (t/testing "column metadata and constraints round-trip via gpkg_schema"
    (with-temp-file f
      (sut/write
       f "test-table"
       [{"geometry" (g/make-point 1 2) "id" 1 "pop" 10 "colour" "red"}
        {"geometry" (g/make-point 4 5) "id" 2 "pop" 20 "colour" "green"}]
       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}
        "pop" {:type :integer
               :title "Population"
               :description "resident count"
               :constraint "pop_range"}
        "colour" {:type :string
                  :name "colour_alias"
                  :mime-type "text/plain"
                  :constraint "colours"}}
       :constraints
       [{:name "pop_range" :type :range :min 0 :max 100
         :min-inclusive? true :max-inclusive? false :description "0..100"}
        {:name "colours" :type :enum :values ["red" "green" "blue"]}])

      (t/testing "gpkg_extensions declares gpkg_schema"
        (let [exts (set (map :extension-name (sut/extensions f)))]
          (t/is (contains? exts "gpkg_schema"))))

      (t/testing "column-metadata reads back per-column details"
        (t/is (= {"pop" {:title "Population"
                         :description "resident count"
                         :constraint "pop_range"}
                  "colour" {:name "colour_alias"
                            :mime-type "text/plain"
                            :constraint "colours"}}
                 (sut/column-metadata f "test-table"))))

      (t/testing "column-constraints reads back constraint definitions"
        (let [cs (sut/column-constraints f)]
          (t/testing "enum expands to one row per value"
            (t/is (= [{:type :enum :value "red"}
                      {:type :enum :value "green"}
                      {:type :enum :value "blue"}]
                     (get cs "colours"))))
          (t/testing "range carries bounds and inclusivity"
            (let [r (first (get cs "pop_range"))]
              (t/is (= 1 (count (get cs "pop_range"))))
              (t/is (= :range (:type r)))
              (t/is (== 0 (:min r)))
              (t/is (== 100 (:max r)))
              (t/is (true? (:min-inclusive? r)))
              (t/is (false? (:max-inclusive? r)))
              (t/is (= "0..100" (:description r)))))))

      (t/testing "the data is still readable"
        (with-open [in (sut/open f :table-name "test-table")]
          (t/is (= #{1 2}
                   (set (map #(get % "id") (sut/features in))))))))))

(t/deftest test-no-schema-extension
  (t/testing "a file written without schema info opens and reports no metadata"
    (with-temp-file f
      (sut/write
       f "test-table"
       [{"geometry" (g/make-point 1 2) "id" 1}]
       :schema {"geometry" {:type :point :srid 27700}
                "id" {:type :integer}})
      (t/is (= {} (sut/column-metadata f "test-table")))
      (t/is (= {} (sut/column-constraints f)))
      (t/is (not (contains? (set (map :extension-name (sut/extensions f)))
                            "gpkg_schema")))
      (with-open [in (sut/open f :table-name "test-table")]
        (t/is (= [1] (map #(get % "id") (sut/features in))))))))

(comment
  (with-open [gpkg (sut/open "/tmp/hnzp-1966294190446547555/Data/oproad_gb.gpkg" :table-name "road_link")
              f (io/writer "/tmp/hnzp-1966294190446547555/test.txt")]
    (doseq [row (sut/features gpkg)]
      (.write f (get row "id"))
      (.write f "\n")))
  )


