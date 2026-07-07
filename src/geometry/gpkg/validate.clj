(ns geometry.gpkg.validate
  "An implementation of the automatable parts of the GeoPackage 1.4
   abstract test suite (Annex A) as a validator for `.gpkg` files.

   `(validate path)` returns a vector of result maps, one per test:

     {:id       \"/base/core/container/data/file_format\"
      :req      1                 ; requirement number in the spec
      :category :core             ; :core :features :attributes :extensions
      :status   :pass             ; :pass :fail :not-applicable :skip
      :message  \"valid SQLite 3 header\"}

   `(valid? path)` is true when no test has :status :fail.

   Only the Core, Features, Attributes and Extension-mechanism options are
   covered. Tiles are out of scope (this library has no tile support), and
   tests that the spec marks as manual inspection are reported as :skip."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [geometry.core :as g]
            [geometry.gpkg.encode :as encode])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.nio.charset StandardCharsets]
           [org.locationtech.jts.geom Geometry]))

;; ---------------------------------------------------------------------------
;; result + query helpers

(defn- ok   [msg] {:status :pass            :message msg})
(defn- bad  [msg] {:status :fail            :message msg})
(defn- na   [msg] {:status :not-applicable  :message msg})
(defn- skip [msg] {:status :skip            :message msg})

(defn- q
  "Run `sql` with `params`, returning rows as maps with unqualified,
   lower-cased keyword keys (so PRAGMA columns are addressable)."
  [conn sql & params]
  (jdbc/execute! conn (into [sql] params)
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn- q1 [conn sql & params] (first (apply q conn sql params)))

(defn- esc "Escape a string literal for a PRAGMA argument." [s]
  (str/replace (str s) "'" "''"))

(defn- esc-id "Escape a SQL identifier for double-quoting." [s]
  (str/replace (str s) "\"" "\"\""))

(defn- has-table? [conn table]
  (some? (q1 conn "SELECT 1 FROM sqlite_master WHERE type IN ('table','view')
                     AND lower(name) = lower(?)" table)))

(defn- table-info [conn table]
  (q conn (format "PRAGMA table_info('%s')" (esc table))))

(defn- table-columns [conn table]
  (into #{} (map (comp str/lower-case :name)) (table-info conn table)))

(defn- require-columns
  "A table_def-style check: `table` exists and has at least `required` columns."
  [conn table required]
  (if-not (has-table? conn table)
    (bad (str table " does not exist"))
    (let [cols    (table-columns conn table)
          missing (remove cols (map str/lower-case required))]
      (if (seq missing)
        (bad (str table " missing columns: " (str/join ", " missing)))
        (ok (str table " has the required columns"))))))

(def ^:private base-column-types
  #{"BOOLEAN" "TINYINT" "SMALLINT" "MEDIUMINT" "INT" "INTEGER"
    "FLOAT" "DOUBLE" "REAL" "TEXT" "BLOB" "DATE" "DATETIME"})

(def ^:private geometry-type-names
  #{"GEOMETRY" "POINT" "LINESTRING" "POLYGON" "MULTIPOINT" "MULTILINESTRING"
    "MULTIPOLYGON" "GEOMETRYCOLLECTION" "CIRCULARSTRING" "COMPOUNDCURVE"
    "CURVEPOLYGON" "MULTICURVE" "MULTISURFACE" "CURVE" "SURFACE"})

(defn- normalize-type [t]
  (-> (or t "") str/upper-case (str/replace #"\(.*\)" "") str/trim))

;; ---------------------------------------------------------------------------
;; GeoPackageBinary header parsing (StandardGeoPackageBinary, spec clause 2.1.3)

(defn- parse-gpb-header
  "Parse the 8-byte StandardGeoPackageBinary header of a geometry `blob`."
  [^bytes blob]
  (when (>= (alength blob) 8)
    (let [bb      (ByteBuffer/wrap blob)
          magic0  (bit-and (.get bb 0) 0xff)
          magic1  (bit-and (.get bb 1) 0xff)
          version (bit-and (.get bb 2) 0xff)
          flags   (.get bb 3)
          little? (bit-test flags 0)
          _       (.order bb (if little? ByteOrder/LITTLE_ENDIAN ByteOrder/BIG_ENDIAN))]
      {:magic-ok?     (and (= 0x47 magic0) (= 0x50 magic1))
       :version       version
       :binary-type   (if (bit-test flags 5) 1 0)
       :empty?        (bit-test flags 4)
       :envelope-code (bit-and (unsigned-bit-shift-right flags 1) 0x07)
       :little?       little?
       :srs-id        (.getInt bb 4)})))

;; ---------------------------------------------------------------------------
;; shared feature helpers

(defn- feature-tables [conn]
  (map :table_name
       (q conn "SELECT table_name FROM gpkg_contents WHERE data_type = 'features'")))

(defn- attribute-tables [conn]
  (map :table_name
       (q conn "SELECT table_name FROM gpkg_contents WHERE data_type = 'attributes'")))

(defn- geometry-column-rows [conn]
  (when (has-table? conn "gpkg_geometry_columns")
    (q conn "SELECT table_name, column_name, geometry_type_name, srs_id, z, m
               FROM gpkg_geometry_columns")))

(defn- feature-geom-blobs
  "Seq of {:table :column :declared-type :gc-srs-id :blob} over every
   non-null geometry value in every feature-table geometry column."
  [conn]
  (let [features (set (feature-tables conn))]
    (for [{tn :table_name cn :column_name gt :geometry_type_name sid :srs_id}
          (geometry-column-rows conn)
          :when (contains? features tn)
          row   (q conn (format "SELECT \"%s\" AS g FROM \"%s\"" (esc-id cn) (esc-id tn)))
          :let  [blob (:g row)]
          :when (some? blob)]
      {:table tn :column cn :declared-type gt :gc-srs-id sid :blob blob})))

(defn- integer-pk-problem
  "nil if `table` has an INTEGER primary-key-like column with unique values,
   else a description of the problem (spec Req 29/150/118)."
  [conn table]
  (let [info (table-info conn table)]
    (if (empty? info)
      (str table ": table has no columns")
      (let [pk-col (or (first (filter #(= 1 (long (:pk %))) info)) (first info))
            id     (:name pk-col)]
        (if (not= "INTEGER" (normalize-type (:type pk-col)))
          (str table ": id column '" id "' is type '" (:type pk-col) "', not INTEGER")
          (let [dup (:d (q1 conn (format "SELECT COUNT(*) - COUNT(DISTINCT \"%s\") AS d FROM \"%s\""
                                         (esc-id id) (esc-id table))))]
            (when (and dup (pos? (long dup)))
              (str table ": id column '" id "' has duplicate values"))))))))

;; ---------------------------------------------------------------------------
;; Base / Core (A.1.1)

(defn- check-file-format [{:keys [path]}]
  (with-open [is (io/input-stream path)]
    (let [buf (byte-array 16) n (.read is buf)]
      (if (and (>= n 16)
               (= "SQLite format 3" (String. buf 0 15 StandardCharsets/US_ASCII)))
        (ok "first 16 bytes are 'SQLite format 3'")
        (bad "first 16 bytes are not 'SQLite format 3'")))))

(defn- check-application-id [{:keys [conn]}]
  (let [app (:application_id (q1 conn "PRAGMA application_id"))
        uv  (:user_version   (q1 conn "PRAGMA user_version"))]
    (cond
      (not= 1196444487 app) (bad (str "application_id is " app ", expected 1196444487 (GPKG)"))
      (< (long uv) 10200)   (bad (str "user_version is " uv ", expected >= 10200"))
      :else                 (ok (str "application_id GPKG, user_version " uv)))))

(defn- check-file-extension [{:keys [path]}]
  (if (str/ends-with? (str/lower-case (str path)) ".gpkg")
    (ok "file extension is .gpkg")
    (bad "file extension is not .gpkg")))

(defn- check-table-data-types [{:keys [conn]}]
  (let [tables (map :table_name
                    (q conn "SELECT table_name FROM gpkg_contents
                               WHERE data_type IN ('tiles','features','attributes')"))]
    (if (empty? tables)
      (na "no user tables registered in gpkg_contents")
      (let [allowed  (into base-column-types geometry-type-names)
            offenders (for [t tables
                            {:keys [name type]} (table-info conn t)
                            :let  [nt (normalize-type type)]
                            :when (and (seq nt) (not (allowed nt)))]
                        (str t "." name " : " type))]
        (if (seq offenders)
          (bad (str "columns with invalid data types: " (str/join ", " offenders)))
          (ok "all user-table column types are valid"))))))

(defn- check-file-integrity [{:keys [conn]}]
  (let [rows (q conn "PRAGMA integrity_check")
        v    (some-> rows first vals first)]
    (if (= "ok" v)
      (ok "PRAGMA integrity_check returned ok")
      (bad (str "integrity_check: " (mapv (comp first vals) rows))))))

(defn- check-foreign-key-integrity [{:keys [conn]}]
  (let [rows (q conn "PRAGMA foreign_key_check")]
    (if (empty? rows)
      (ok "no foreign key violations")
      (bad (str (count rows) " foreign key violation(s): " (vec rows))))))

(defn- check-sql-api [{:keys [conn]}]
  (q conn "SELECT * FROM sqlite_master")
  (ok "SQLite SQL API is available"))

(defn- check-srs-table-def [{:keys [conn]}]
  (require-columns conn "gpkg_spatial_ref_sys"
                   ["srs_name" "srs_id" "organization"
                    "organization_coordsys_id" "definition" "description"]))

(defn- check-srs-defaults [{:keys [conn]}]
  (let [neg1 (q1 conn "SELECT 1 AS ok FROM gpkg_spatial_ref_sys
                         WHERE srs_id = -1 AND organization = 'NONE'
                           AND organization_coordsys_id = -1")
        zero (q1 conn "SELECT 1 AS ok FROM gpkg_spatial_ref_sys
                         WHERE srs_id = 0 AND organization = 'NONE'
                           AND organization_coordsys_id = 0")
        epsg (q1 conn "SELECT 1 AS ok FROM gpkg_spatial_ref_sys
                         WHERE lower(organization) = 'epsg'
                           AND organization_coordsys_id = 4326")]
    (cond
      (not neg1) (bad "missing default srs_id = -1 (NONE) row")
      (not zero) (bad "missing default srs_id = 0 (NONE) row")
      (not epsg) (bad "missing EPSG:4326 row")
      :else      (ok "required default SRS rows present"))))

(defn- check-srs-required [{:keys [conn]}]
  (let [rows (q conn "SELECT DISTINCT gc.srs_id AS sid
                        FROM gpkg_contents gc
                        LEFT OUTER JOIN gpkg_spatial_ref_sys srs
                          ON srs.srs_id = gc.srs_id
                       WHERE gc.data_type IN ('tiles','features')
                         AND srs.srs_id IS NULL")]
    (if (empty? rows)
      (ok "every referenced srs_id is defined")
      (bad (str "undefined srs_id values referenced by contents: " (mapv :sid rows))))))

(defn- check-contents-table-def [{:keys [conn]}]
  (require-columns conn "gpkg_contents"
                   ["table_name" "data_type" "identifier" "description"
                    "last_change" "min_x" "min_y" "max_x" "max_y" "srs_id"]))

(defn- check-contents-table-name [{:keys [conn]}]
  (let [rows (q conn "SELECT DISTINCT table_name AS t FROM gpkg_contents
                       WHERE table_name NOT IN (SELECT name FROM sqlite_master)")]
    (if (empty? rows)
      (ok "every gpkg_contents.table_name exists")
      (bad (str "gpkg_contents references missing tables: " (mapv :t rows))))))

(def ^:private iso8601-re #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")

(defn- check-contents-last-change [{:keys [conn]}]
  (let [vals (map :last_change (q conn "SELECT last_change FROM gpkg_contents"))]
    (if (empty? vals)
      (na "no gpkg_contents rows")
      (let [offenders (remove #(re-matches iso8601-re (str %)) vals)]
        (if (seq offenders)
          (bad (str "non-ISO8601 last_change values: " (vec offenders)))
          (ok "all last_change values are ISO8601"))))))

(defn- check-contents-srs-id [{:keys [conn]}]
  (let [rows (q conn "PRAGMA foreign_key_check('gpkg_contents')")]
    (if (empty? rows)
      (ok "gpkg_contents.srs_id references are valid")
      (bad (str "gpkg_contents FK violations: " (vec rows))))))

(defn- check-valid-geopackage [{:keys [conn]}]
  (let [n (long (:n (q1 conn "SELECT COUNT(*) AS n FROM gpkg_contents
                               WHERE data_type IN ('tiles','features')")))]
    (if (pos? n)
      (ok "contains a features or tiles table")
      (bad "no features or tiles table registered in gpkg_contents"))))

;; ---------------------------------------------------------------------------
;; Features (A.2.1)

(defn- check-features-row [{:keys [conn]}]
  (let [tables (feature-tables conn)]
    (if (empty? tables)
      (na "no feature tables")
      (let [problems (keep #(integer-pk-problem conn %) tables)]
        (if (seq problems)
          (bad (str "feature table problems: " (str/join "; " problems)))
          (ok "every feature table exists with an integer primary key"))))))

(defn- check-blob-format [{:keys [conn]}]
  (if (empty? (feature-tables conn))
    (na "no feature tables")
    (let [blobs (feature-geom-blobs conn)]
      (if (empty? blobs)
        (na "no non-null feature geometries")
        (let [problems (for [{:keys [table blob]} blobs
                             :let [h (parse-gpb-header blob)]
                             :when (not (and h (:magic-ok? h) (zero? (:version h))
                                             (zero? (:binary-type h))
                                             (<= 0 (:envelope-code h) 4)))]
                         (str table ": " (select-keys h [:magic-ok? :version :binary-type :envelope-code])))]
          (if (seq problems)
            (bad (str "invalid GeoPackageBinary headers: " (vec problems)))
            (ok "all geometry blobs are valid StandardGeoPackageBinary")))))))

(defn- check-empty-geometry [{:keys [conn]}]
  (if (empty? (feature-tables conn))
    (na "no feature tables")
    (let [blobs (feature-geom-blobs conn)]
      (if (empty? blobs)
        (na "no non-null feature geometries")
        (let [problems (for [{:keys [table blob]} blobs
                             :let [h (parse-gpb-header blob)]
                             :when (and h (:empty? h) (not (zero? (:envelope-code h))))]
                         (str table ": empty geometry with non-empty envelope"))]
          (if (seq problems)
            (bad (str "inconsistent empty geometry encoding: " (vec problems)))
            (ok "empty geometries encoded consistently")))))))

(defn- check-core-types [{:keys [conn]}]
  (if (empty? (feature-tables conn))
    (na "no feature tables")
    (let [blobs (feature-geom-blobs conn)]
      (if (empty? blobs)
        (na "no non-null feature geometries")
        (let [rdr (encode/reader g/*factory*)
              problems (for [{:keys [table blob]} blobs
                             :let [ok? (try (some? (encode/decode blob rdr))
                                            (catch Throwable t (.getMessage t)))]
                             :when (not (true? ok?))]
                         (str table ": " (if (string? ok?) ok? "WKB did not decode")))]
          (if (seq problems)
            (bad (str "geometries not valid GeoPackageBinary WKB: " (vec problems)))
            (ok "all geometries decode as valid WKB")))))))

(defn- check-gc-table-def [{:keys [conn]}]
  (if (empty? (feature-tables conn))
    (na "no feature tables")
    (require-columns conn "gpkg_geometry_columns"
                     ["table_name" "column_name" "geometry_type_name" "srs_id" "z" "m"])))

(defn- check-gc-one-row-per-table [{:keys [conn]}]
  (if (empty? (feature-tables conn))
    (na "no feature tables")
    (let [rows (q conn "SELECT table_name AS t FROM gpkg_contents
                         WHERE data_type = 'features'
                           AND table_name NOT IN (SELECT table_name FROM gpkg_geometry_columns)")]
      (if (empty? rows)
        (ok "every feature table has a gpkg_geometry_columns row")
        (bad (str "feature tables missing from gpkg_geometry_columns: " (mapv :t rows)))))))

(defn- check-gc-table-name-fk [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_geometry_columns")
    (na "no gpkg_geometry_columns table")
    (let [fks (q conn "PRAGMA foreign_key_list('gpkg_geometry_columns')")]
      (if (some (fn [r] (and (= "gpkg_contents" (str/lower-case (str (:table r))))
                             (= "table_name"    (str/lower-case (str (:from r))))))
                fks)
        (ok "table_name is a foreign key to gpkg_contents")
        (bad "gpkg_geometry_columns.table_name is not a foreign key to gpkg_contents")))))

(defn- check-gc-column-name [{:keys [conn]}]
  (let [rows (geometry-column-rows conn)]
    (if (empty? rows)
      (na "no gpkg_geometry_columns rows")
      (let [problems (for [{tn :table_name cn :column_name} rows
                           :when (not (contains? (table-columns conn tn) (str/lower-case (str cn))))]
                       (str tn "." cn))]
        (if (seq problems)
          (bad (str "geometry column names not present in their tables: " (vec problems)))
          (ok "every geometry column name exists in its table"))))))

(defn- check-gc-geometry-type [{:keys [conn]}]
  (let [rows (geometry-column-rows conn)]
    (if (empty? rows)
      (na "no gpkg_geometry_columns rows")
      (let [bad-types (for [{gt :geometry_type_name} rows
                            :when (not (geometry-type-names (str/upper-case (str gt))))]
                        gt)]
        (if (seq bad-types)
          (bad (str "invalid geometry_type_name values: " (vec bad-types)))
          (ok "all geometry_type_name values are valid"))))))

(defn- check-gc-srs-id [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_geometry_columns")
    (na "no gpkg_geometry_columns table")
    (let [rows (q conn "PRAGMA foreign_key_check('gpkg_geometry_columns')")]
      (if (empty? rows)
        (ok "gpkg_geometry_columns.srs_id references are valid")
        (bad (str "gpkg_geometry_columns FK violations: " (vec rows)))))))

(defn- check-gc-srs-id-match [{:keys [conn]}]
  (let [rows (geometry-column-rows conn)]
    (if (empty? rows)
      (na "no gpkg_geometry_columns rows")
      (let [mismatches (q conn "SELECT gc.table_name AS t FROM gpkg_geometry_columns gc
                                  JOIN gpkg_contents c ON gc.table_name = c.table_name
                                 WHERE gc.srs_id <> c.srs_id")]
        (if (empty? mismatches)
          (ok "srs_id matches between gpkg_geometry_columns and gpkg_contents")
          (bad (str "srs_id mismatch for tables: " (mapv :t mismatches))))))))

(defn- check-gc-z [{:keys [conn]}]
  (let [rows (geometry-column-rows conn)]
    (if (empty? rows)
      (na "no gpkg_geometry_columns rows")
      (let [bad-vals (remove #(#{0 1 2} (some-> (:z %) long)) rows)]
        (if (seq bad-vals)
          (bad (str "invalid z values: " (mapv :z bad-vals)))
          (ok "all z values are in {0,1,2}"))))))

(defn- check-gc-m [{:keys [conn]}]
  (let [rows (geometry-column-rows conn)]
    (if (empty? rows)
      (na "no gpkg_geometry_columns rows")
      (let [bad-vals (remove #(#{0 1 2} (some-> (:m %) long)) rows)]
        (if (seq bad-vals)
          (bad (str "invalid m values: " (mapv :m bad-vals)))
          (ok "all m values are in {0,1,2}"))))))

(defn- check-feature-table-pk [{:keys [conn]}]
  (let [tables (feature-tables conn)]
    (if (empty? tables)
      (na "no feature tables")
      (let [problems (keep #(integer-pk-problem conn %) tables)]
        (if (seq problems)
          (bad (str/join "; " problems))
          (ok "every feature table has an integer primary key with unique values"))))))

(defn- check-one-geometry-column [{:keys [conn]}]
  (let [tables (feature-tables conn)]
    (if (empty? tables)
      (na "no feature tables")
      (let [problems (for [t tables
                           :let [n (count (filter #(= t (:table_name %)) (geometry-column-rows conn)))]
                           :when (> n 1)]
                       (str t " has " n " geometry columns"))]
        (if (seq problems)
          (bad (str "feature tables with multiple geometry columns: " (vec problems)))
          (ok "every feature table has exactly one geometry column"))))))

(defn- check-geometry-column-type [{:keys [conn]}]
  (let [rows (filter #(contains? (set (feature-tables conn)) (:table_name %))
                     (geometry-column-rows conn))]
    (if (empty? rows)
      (na "no feature geometry columns")
      (let [problems (for [{tn :table_name cn :column_name gt :geometry_type_name} rows
                           :let [decl (some->> (table-info conn tn)
                                               (filter #(= (str/lower-case (str cn))
                                                           (str/lower-case (str (:name %)))))
                                               first :type normalize-type)]
                           :when (not= (str/upper-case (str gt)) decl)]
                       (str tn "." cn " declared '" decl "', expected '" (str/upper-case (str gt)) "'"))]
        (if (seq problems)
          (bad (str "geometry column SQL type mismatch: " (vec problems)))
          (ok "geometry column SQL types match geometry_type_name"))))))

(defn- jts-type-name [^Geometry geom]
  (str/upper-case (.getGeometryType geom)))

(defn- check-data-geometry-type [{:keys [conn]}]
  (if (empty? (feature-tables conn))
    (na "no feature tables")
    (let [blobs (feature-geom-blobs conn)]
      (if (empty? blobs)
        (na "no non-null feature geometries")
        (let [rdr (encode/reader g/*factory*)
              problems (for [{:keys [table declared-type blob]} blobs
                             :let [decl (str/upper-case (str declared-type))
                                   actual (try (some-> (encode/decode blob rdr) jts-type-name)
                                               (catch Throwable _ nil))]
                             :when (and (not= "GEOMETRY" decl) actual (not= decl actual))]
                         (str table ": declared " decl ", found " actual))]
          (if (seq problems)
            (bad (str "geometry type mismatches: " (vec problems)))
            (ok "feature geometry types match geometry_type_name")))))))

(defn- check-data-geometry-srs [{:keys [conn]}]
  (if (empty? (feature-tables conn))
    (na "no feature tables")
    (let [blobs (feature-geom-blobs conn)]
      (if (empty? blobs)
        (na "no non-null feature geometries")
        (let [problems (for [{:keys [table gc-srs-id blob]} blobs
                             :let [h (parse-gpb-header blob)]
                             :when (and h (not= (long gc-srs-id) (long (:srs-id h))))]
                         (str table ": blob srs_id " (:srs-id h) ", expected " gc-srs-id))]
          (if (seq problems)
            (bad (str "geometry srs_id mismatches: " (vec problems)))
            (ok "feature geometry srs_id values match gpkg_geometry_columns")))))))

;; ---------------------------------------------------------------------------
;; Extension mechanism (A.2.3)

(def ^:private known-gpkg-extensions
  #{"gpkg_rtree_index" "gpkg_schema" "gpkg_zoom_other" "gpkg_webp"
    "gpkg_metadata" "gpkg_crs_wkt" "gpkg_srs_id_trigger" "gpkg_elevation_tiles"
    "gpkg_geometry_type_trigger" "gpkg_geometry_columns" "gpkg_related_tables"})

(defn- ext-rows [conn]
  (q conn "SELECT table_name, column_name, extension_name, definition, scope
             FROM gpkg_extensions"))

(defn- check-ext-table-def [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_extensions")
    (na "no gpkg_extensions table")
    (require-columns conn "gpkg_extensions"
                     ["table_name" "column_name" "extension_name" "definition" "scope"])))

(defn- check-ext-table-name [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_extensions")
    (na "no gpkg_extensions table")
    (let [rows (ext-rows conn)]
      (if (empty? rows)
        (na "no gpkg_extensions rows")
        (let [problems (for [{tn :table_name} rows
                             :when (and (some? tn) (not (has-table? conn tn)))]
                         tn)]
          (if (seq problems)
            (bad (str "gpkg_extensions references missing tables: " (vec problems)))
            (ok "every extension table_name exists")))))))

(defn- check-ext-column-name [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_extensions")
    (na "no gpkg_extensions table")
    (let [rows (ext-rows conn)]
      (if (empty? rows)
        (na "no gpkg_extensions rows")
        (let [problems (for [{tn :table_name cn :column_name} rows
                             :when (and (some? cn) (some? tn)
                                        (not (contains? (table-columns conn tn) (str/lower-case (str cn)))))]
                         (str tn "." cn))]
          (if (seq problems)
            (bad (str "gpkg_extensions references missing columns: " (vec problems)))
            (ok "every extension column_name exists")))))))

(defn- extension-name-problem [ename]
  (let [e (str ename)]
    (cond
      (known-gpkg-extensions e) nil
      :else
      (let [i (str/index-of e "_")]
        (if-not i
          (str e " (no author_extension form)")
          (let [author (subs e 0 i)
                ext    (subs e (inc i))]
            (cond
              (= "gpkg" author)                        (str e " (unregistered gpkg author)")
              (not (re-matches #"[a-zA-Z0-9]+" author)) (str e " (invalid author)")
              (not (re-matches #"[a-zA-Z0-9_]+" ext))   (str e " (invalid extension)")
              :else nil)))))))

(defn- check-ext-name [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_extensions")
    (na "no gpkg_extensions table")
    (let [rows (ext-rows conn)]
      (if (empty? rows)
        (na "no gpkg_extensions rows")
        (let [problems (keep #(extension-name-problem (:extension_name %)) rows)]
          (if (seq problems)
            (bad (str "invalid extension_name values: " (vec problems)))
            (ok "all extension_name values are valid")))))))

(defn- check-ext-definition [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_extensions")
    (na "no gpkg_extensions table")
    (let [rows (ext-rows conn)]
      (if (empty? rows)
        (na "no gpkg_extensions rows")
        (let [problems (for [{d :definition} rows
                             :when (not (re-find #"(?i)^(annex |https?://|mailto:|extension title)"
                                                 (str/trim (str d))))]
                         d)]
          (if (seq problems)
            (bad (str "extension definitions not referencing documentation: " (vec problems)))
            (ok "all extension definitions reference documentation")))))))

(defn- check-ext-scope [{:keys [conn]}]
  (if-not (has-table? conn "gpkg_extensions")
    (na "no gpkg_extensions table")
    (let [rows (ext-rows conn)]
      (if (empty? rows)
        (na "no gpkg_extensions rows")
        (let [problems (remove #(#{"read-write" "write-only"} (:scope %)) rows)]
          (if (seq problems)
            (bad (str "invalid scope values: " (mapv :scope problems)))
            (ok "all extension scope values are valid")))))))

;; ---------------------------------------------------------------------------
;; Attributes (A.2.4)

(defn- check-attributes-row [{:keys [conn]}]
  (let [tables (attribute-tables conn)]
    (if (empty? tables)
      (na "no attributes tables")
      (let [problems (keep #(integer-pk-problem conn %) tables)]
        (if (seq problems)
          (bad (str "attributes table problems: " (str/join "; " problems)))
          (ok "every attributes table exists with an integer primary key"))))))

;; ---------------------------------------------------------------------------
;; registry + entry points

(def ^:private checks
  [{:id "/base/core/container/data/file_format"              :req 1   :category :core :check check-file-format}
   {:id "/base/core/container/data/file_format/application_id" :req 2 :category :core :check check-application-id}
   {:id "/base/core/container/data/file_extension_name"      :req 3   :category :core :check check-file-extension}
   {:id "/base/core/container/data/table_data_types"         :req 5   :category :core :check check-table-data-types}
   {:id "/base/core/container/data/file_integrity"           :req 6   :category :core :check check-file-integrity}
   {:id "/base/core/container/data/foreign_key_integrity"    :req 7   :category :core :check check-foreign-key-integrity}
   {:id "/base/core/container/api/sql"                        :req 8   :category :core :check check-sql-api}
   {:id "/base/core/gpkg_spatial_ref_sys/data/table_def"     :req 10  :category :core :check check-srs-table-def}
   {:id "/base/core/gpkg_spatial_ref_sys/data_values_default" :req 11 :category :core :check check-srs-defaults}
   {:id "/base/core/spatial_ref_sys/data_values_required"    :req 12  :category :core :check check-srs-required}
   {:id "/base/core/contents/data/table_def"                 :req 13  :category :core :check check-contents-table-def}
   {:id "/base/core/contents/data/data_values_table_name"    :req 14  :category :core :check check-contents-table-name}
   {:id "/base/core/contents/data/data_values_last_change"   :req 15  :category :core :check check-contents-last-change}
   {:id "/base/core/contents/data/data_values_srs_id"        :req 16  :category :core :check check-contents-srs-id}
   {:id "/opt/valid_geopackage"                              :req 17  :category :core :check check-valid-geopackage}

   {:id "/opt/features/contents/data/features_row"                       :req 18  :category :features :check check-features-row}
   {:id "/opt/features/geometry_encoding/data/blob"                      :req 19  :category :features :check check-blob-format}
   {:id "/opt/features/geometry_encoding/data/empty_geometry"           :req 152 :category :features :check check-empty-geometry}
   {:id "/opt/features/geometry_encoding/data/core_types_existing_sparse_data" :req 20 :category :features :check check-core-types}
   {:id "/opt/features/geometry_columns/data/table_def"                 :req 21  :category :features :check check-gc-table-def}
   {:id "/opt/features/geometry_columns/data/data_values_geometry_columns" :req 22 :category :features :check check-gc-one-row-per-table}
   {:id "/opt/features/geometry_columns/data/data_values_table_name"    :req 23  :category :features :check check-gc-table-name-fk}
   {:id "/opt/features/geometry_columns/data/data_values_column_name"   :req 24  :category :features :check check-gc-column-name}
   {:id "/opt/features/geometry_columns/data/data_values_geometry_type_name" :req 25 :category :features :check check-gc-geometry-type}
   {:id "/opt/features/geometry_columns/data/data_values_srs_id"        :req 26  :category :features :check check-gc-srs-id}
   {:id "/opt/features/geometry_columns/data/data_values_srs_id_match"  :req 146 :category :features :check check-gc-srs-id-match}
   {:id "/opt/features/geometry_columns/data/data_values_z"             :req 27  :category :features :check check-gc-z}
   {:id "/opt/features/geometry_columns/data/data_values_m"             :req 28  :category :features :check check-gc-m}
   {:id "/opt/features/vector_features/data/feature_table"              :req 29  :category :features :check check-feature-table-pk}
   {:id "/opt/features/vector_features/data/feature_table_one_geometry_column" :req 30 :category :features :check check-one-geometry-column}
   {:id "/opt/features/vector_features/data/feature_table_geometry_column_type" :req 31 :category :features :check check-geometry-column-type}
   {:id "/opt/features/vector_features/data/data_values_geometry_type"  :req 32  :category :features :check check-data-geometry-type}
   {:id "/opt/features/vector_features/data/data_value_geometry_srs_id" :req 33  :category :features :check check-data-geometry-srs}

   {:id "/opt/extension_mechanism/data/table_def"                    :req 58 :category :extensions :check check-ext-table-def}
   {:id "/opt/extension_mechanism/data/data_values_for_extensions"   :req 59 :category :extensions
    :check (fn [_] (skip "requires manual inspection of extensions in use"))}
   {:id "/opt/extension_mechanism/data/data_values_table_name"       :req 60 :category :extensions :check check-ext-table-name}
   {:id "/opt/extension_mechanism/data/data_values_column_name"      :req 61 :category :extensions :check check-ext-column-name}
   {:id "/opt/extension_mechanism/data/data_values_extension_name"   :req 62 :category :extensions :check check-ext-name}
   {:id "/opt/extension_mechanism/data/data_values_definition"       :req 63 :category :extensions :check check-ext-definition}
   {:id "/opt/extension_mechanism/data/data_values_scope"            :req 64 :category :extensions :check check-ext-scope}

   {:id "/opt/attributes/contents/data/attributes_row" :req 118 :category :attributes :check check-attributes-row}])

(defn validate
  "Run the abstract test suite against the GeoPackage at `path`. Returns a
   vector of result maps, one per test (see the namespace docstring)."
  [path]
  (with-open [conn (jdbc/get-connection
                    (jdbc/get-datasource {:jdbcUrl (str "jdbc:sqlite:" path)}))]
    (let [ctx {:conn conn :path (str path)}]
      (mapv (fn [{:keys [id req category check]}]
              (let [r (try (check ctx)
                           (catch Throwable t
                             (bad (str "exception: " (.getMessage t)))))]
                (merge {:id id :req req :category category} r)))
            checks))))

(defn failures
  "The subset of `(validate path)` results whose status is :fail."
  [path]
  (filterv #(= :fail (:status %)) (validate path)))

(defn valid?
  "True when no abstract test fails for the GeoPackage at `path`."
  [path]
  (empty? (failures path)))
