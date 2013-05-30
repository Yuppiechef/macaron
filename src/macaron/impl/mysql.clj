(ns macaron.impl.mysql
  (:use [clojure.tools.logging])
  (:require [clojure.java.jdbc :as sql])
  (:require [clojure.string :as string])
  (:require [clojure.set :as set]))

(def coltypes
  {:version "BIGINT DEFAULT 0 NOT NULL"
   :currency "BIGINT"
   :auto-index "BIGINT PRIMARY KEY AUTO_INCREMENT"
   :uuid "varchar(36)"
   :unique-uuid "varchar(36) UNIQUE"
   :full-varchar "varchar(255)"
   :password "varchar(32)"
   :enum "ENUM"
   :email "varchar(255)"
   :ipaddress "varchar(32)"
   :link "BIGINT"
   :boolean "tinyint(1)"
   :date "DATETIME"})

(def default-indextypes
  {:uuid :unique
   :text :fulltext
   :mediumtext :fulltext
   :longtext :fulltext})

(defn current-database
  "Returns the currently selected database name"
  []
  (sql/with-query-results rs ["SELECT DATABASE() as db"]
    (:db (first rs))))

(defn filter-existing
  "Filters out existing columns for the update-table function"
  [specs existing]
  (filter (fn [col] (nil? (some #{(sql/as-identifier (first col))} existing))) specs))

(defn table-exists [table-name]
  (not (nil? (sql/with-query-results rs [(str "SHOW TABLES LIKE \"" table-name "\"")] (first rs)))))

(defn index-exists?
  "Check if a named index exists on a table"
  [table-name index-name]
  (sql/with-query-results rs [ (str "SHOW INDEX FROM " table-name " WHERE key_name=?") index-name]
    (not (empty? rs))))

(defn add-index
  "Add an index to a table if it doesn't exist, just silently ignore if it does though"
  ([table-name column-name]
     (add-index table-name column-name [[column-name]]))
  ([table-name index-name columns]
     (add-index table-name index-name "" columns))
  ([table-name index-name index-type columns]
     (let [coldefs (apply str
                          (map sql/as-identifier
                               (apply concat
                                      (interpose ", "
                                                 (map (partial interpose " ") columns)))))
           index-type (sql/as-identifier index-type)
           index-name (sql/as-identifier index-name)
           alterstr (str "ALTER TABLE " table-name " ADD INDEX " index-name " (" coldefs ")")]
       (if (index-exists? table-name index-name)
         false
         (do
           (sql/do-commands alterstr))
         ))))

(defn fk-exists?
  "Check if a named foreign key exists on a table"
  [table-name fk-name]
  (sql/with-query-results rs ["SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE referenced_table_name is not null AND table_schema=? AND TABLE_NAME=? AND CONSTRAINT_NAME=?" (current-database) table-name fk-name]
    (not (empty? rs))))

(defn add-foreignkey
  "Adds a foreign key link to a table if it doesn't exist. Ignores silently if it does exist."
  [table-name column-name fk-table-name fk-column-name optionmap]
  (cond
   (nil? fk-table-name) (throw (RuntimeException. (str "No foreign key table for " table-name "." column-name " link")))
   :else
   (let [fk-name (str table-name "_" column-name "_fk")
         reference (str fk-table-name "(" fk-column-name ")")
         on-delete (-> (get optionmap :delete :restrict) name .toUpperCase)
         on-update (-> (get optionmap :update :restrict) name .toUpperCase)
         alterstr (str "ALTER TABLE " table-name " ADD CONSTRAINT " fk-name" FOREIGN KEY " fk-name " (" column-name ") REFERENCES " reference " ON DELETE " on-delete " ON UPDATE " on-update)]
     (if (fk-exists? table-name fk-name)
       false
       (do
         (sql/do-commands alterstr))))))

(defn check-enum-columns
  "Checking if a column's type is an enum and include it in the returned collection"
  [columns column]
  (if (.startsWith (:type column) "enum")
    (conj columns column)
    columns))

(defn get-enum-specs 
  "Return the enum specs for matching enum fields"
  [specs fields]
  (reduce 
   (fn [allspecs currentspec]
     (if (not (nil? (some #{(name (first currentspec))} fields)))
       (conj allspecs currentspec)
       allspecs))
    (list) 
    specs))

(defn get-existing-enum-types
  "Find the current enum and return its current types"
  [name enums]
  (reduce
   (fn [all current]
     (if (= (:field current) name)
             (conj all (:type current))
             all))
   (list)
   enums))

(defn merge-types
  "Strips out just the names of the enum types from a string with types, commas and spaces"
  [types]
  (reduce
   (fn [all f]
     (if-not (= (.trim f) ",")
       (conj all (str "'" (.trim f) "'"))
       all))
   (hash-set) types))

(defn parse-types
  "Meh function to strip out the types from the spec or column definition string. Skips first 6 letters and the last letter to remove enum( ) "
  [enum]
  (string/split (subs enum 6 (- (.length enum) 1)) #"\'"))

(defn merge-enum-types
  "Merged new and existing enum types together, eg: enum('boy','girl') and ENUM('male', 'female') returns enum('boy', 'girl', 'male', 'female')"
  [new existing]
  (let [existing-enums (merge-types (parse-types existing))
        new-enums (merge-types (parse-types new))
        missing (set/difference new-enums existing-enums)        
        types (set/union new-enums existing-enums)
        fields (apply str (concat (interpose "," types)))]
    (if (> (count missing) 0)      
      (str "enum(" fields ")"))))

(defn merge-enums
  "Merge old enum values into the new version of the enum so that old types are preserved but the new enum types are added
   Eg: existing: {:field gender, :type enum('boy','girl'), :null YES, :key , :default nil, :extra } and new: [:gender ENUM('male', 'female')]"
  [new existing]
  (reduce
   (fn [all n]
     (let [existing-types-as-string (first (get-existing-enum-types (name (first n)) existing))
           merged-types (merge-enum-types (second n) existing-types-as-string)]
       (if-not (nil? merged-types)
         (conj all (vector (first n) merged-types))
         all)))
       {}
       new))

(defn add-enum-name
  "Hack to add the enum's name to its spec in the front so the correct sql will be generated
   Eg: [:gender ENUM('boy', 'girl')] converts to [:gender :gender ENUM('boy', 'girl')]"
  [all current]
  (conj all (into (vector (first current)) current)))

(defn enum-specs-to-string
  "Convert the enum into a sql query string so the query can be run to update all the table's enums"
  [enum-specs existing-enums]
  (let [merged-enums (merge-enums enum-specs existing-enums)
        specs (reduce add-enum-name (list) merged-enums)]
    (if (> (count merged-enums) 0)
      (apply str
             (map sql/as-identifier
                  (apply concat
                         (interpose [", CHANGE "]
                                    (map (partial interpose " ") specs)))))
      nil)))

(defn colname
  "Transform to valid column name"
  [col]
  (.replaceAll (name col) "-" "_"))

(defn update-table 
  "Adds non-existing columns to a table on the open database connection given a table name and
  specs. This can be used instead of create-table Each spec is either a column spec: a vector containing a column
  name and optionally a type and other constraints, or a table-level
  constraint: a vector containing words that express the constraint.  All words used to
  describe the table may be supplied as strings or keywords."  
  [table-name & specs]
  (def exists (table-exists (colname table-name)))
  (let [split-specs (partition-by #(= :table-spec %) specs)
        col-specs (first split-specs)
        table-spec (first (second (rest split-specs)))
        table-spec-str (or (and table-spec (str " " table-spec)) "")]
                                        ; Create the table with placeholder column (to remove after other columns are added.
    (if (not exists)
      (sql/do-commands (str "CREATE TABLE " table-name " (__placeholder__ int) ENGINE=innodb, CHARACTER SET=utf8, COLLATE=utf8_general_ci" table-spec-str)))
    
    (sql/with-query-results rs [(str "SHOW COLUMNS FROM " table-name)]
      (let [existing-columns (map #(:field %) rs)
            remaining-cols (filter-existing specs existing-columns)
            specs-to-string (fn [specs]
                              (apply str
                                     (map sql/as-identifier
                                          (apply concat
                                                 (interpose [", ADD COLUMN "]
                                                            (map (partial interpose " ") remaining-cols))))))
            query (format "ALTER TABLE %s ADD COLUMN %s %s"
                          (sql/as-identifier table-name)
                          (specs-to-string col-specs)
                          table-spec-str)
            existing-enums (reduce check-enum-columns (list) rs)
            existing-enum-names (map #(:field %) existing-enums)
            enum-col-specs (get-enum-specs col-specs existing-enum-names)
            enum-sql (enum-specs-to-string enum-col-specs existing-enums)
            enum-query (format "ALTER TABLE %s CHANGE %s %s"
                               (sql/as-identifier table-name)
                               enum-sql
                               table-spec-str)]
        (when (> (count remaining-cols) 0)
          (sql/do-commands query))
        (when (not (nil? enum-sql))
          (sql/do-commands enum-query))                   
        ))
    (if (not exists)
      (sql/do-commands (str "ALTER TABLE " table-name " DROP COLUMN __placeholder__")))))

(defn map-colname [fielddesc]
  (keyword (colname (first fielddesc))))

(defn index-colnames [[col link flds]]
  [(keyword (colname col)) link (map #(into [(keyword (colname (first %)))] (rest %)) flds)])

(defn convert-field-to-index
  "Convert a single field to index definition"
  [fld]
  (let [fldname (first fld)
        fldtype (second fld)
        indextypes (some #{:index :unique :fulltext :spatial} fld)
        indextype (cond
                   (not-empty indextypes) (first indextypes)
                   (contains? default-indextypes fldtype) (default-indextypes fldtype)
                   :else :index)]
    [fldname indextype [[fldname]]]))

(defn index-fields
  "Get a list of indexed fields on a list of column definitions and transform it to [idxname idxtype & cols]"
  [flds]
  (map convert-field-to-index (filter #(some #{:indexed :link} (nth % 1)) flds)))

(defn find-indices
  "Get a list of all the indices that should be maintained for this entity"
  [entitydef]
  (let [indices (:indices entitydef)
        indices (map (fn [[k [t o]]] [k t o]) indices)
        flds (:fields entitydef)
        indexed-fields (index-fields flds)]
    (map index-colnames (into indices indexed-fields))))

(defn convert-field-to-fk
  "Convert a single field definition to foreign key"
  [[col [tp foreign-ent opts]]]
  (let [column (name col)
        fk-table (:tablename foreign-ent)
        fk-column (name (:column opts :id))]
    [(colname column) fk-table (colname fk-column) opts]))

(defn find-foreignkeys
  "Get a list of all foreign keys that should be maintained for this entity"
  [entitydef]
  (map convert-field-to-fk (filter #(some #{:link} (nth % 1)) (:fields entitydef))))

(defn join-str-with [prefix sep coll suffix]
  (str prefix (apply str (interpose sep coll)) suffix))

(defn get-enum-value [cell]
  (cond
   (= (first cell) 'quote) (eval (eval cell))
   :else cell))

(defn map-coltype [[name [coltype opts]]]
  (cond
   (= coltype :enum) (join-str-with "ENUM('" "', '" (get-enum-value opts) "')")
   (contains? coltypes coltype) (coltypes coltype)
   :else coltype))

(defn update-entity-table
  "Get the actions for updating entity table"
  [entitydef]
  (let [enname (:name entitydef)
        tablename (:tablename entitydef)
        exists (table-exists tablename)
        flds (:fields entitydef)
        fielddesc (map #(vector (map-colname %) (map-coltype %)) flds)]

    {;; Update columns
     :entity entitydef
     :table
     #(apply update-table (colname tablename) fielddesc)
     
     ;; Apply indices
     :indices
     #(doseq [index (find-indices entitydef)]
        (apply add-index tablename index))

     ;; Apply foreign keys
     :fk
     #(doseq [fk (find-foreignkeys entitydef)]
        (apply add-foreignkey tablename fk))

     ;; Post create function if applicable
     :post-create
     #(if (not exists)
        (if-let [post-fns (:post-create-fn entitydef)]
          (doseq [post-fn post-fns]
            (post-fn entitydef))))
     }))

(defn batch-update-entities [entitylist]
  (let [tables (doall (for [def entitylist]
                        (update-entity-table (second def))))]
    (doseq [op [:table :indices :fk :post-create]
            t tables]
      ((op t)))))

(defn param-value
  "Get the value of a key on param map, or throw a runtime exception if not found. Fail-fast."
  [entname querykey param-map qry pmap paramkey]
  (if (contains? param-map paramkey)
    (let [result (param-map paramkey)]
      (if (coll? result)
        (if (not-empty result)
          (apply conj pmap result))
        (conj pmap result)))
    (throw (new RuntimeException
                (str paramkey " not passed for query on "
                     entname ", " querykey " with param map "
                     param-map " on query: " qry)))))

(defn substitute-param-keys
  "Replace :param-keys with ?'s - If the param-value in question is a collection, it will create a ? for each entry."
  [query param-map]
  (reduce
   (fn [acc section]
     (cond
      (keyword? section)
      (let [value (get param-map section "")]
        (str acc
             (if (coll? value)
               (join-str-with "" "," (repeat (count value) "?") "")
               "?")))
      :else (str acc section)))
   "" query))

(defn get-query-paramkeys
  "Gets the parameters of a query as keys, ordered by match"
  [qry]
  (map (comp keyword second) (re-seq #":([A-Za-z0-9+*_-]+)" qry)))

(defn interweave [c1 c2]
  (let [s1 (seq c1) s2 (seq c2)]
    (cond
     (nil? s1) s2
     (nil? s2) s1
     :else (lazy-seq
            (cons (first s1) (cons (first s2) (interweave (rest s1) (rest s2))))))))

(defn get-named-query
  "Get a named query from an entity and return the SQL + parameters ready for use in sql/with-query-results."
  [{name :name queries :queries} querykey param-map]
  (if-let [query (get queries querykey)]
    (let [paramkeys (:params query)
          paramquery (substitute-param-keys (:woven query) param-map)
          pmapping (partial param-value name querykey param-map query)]
      (into [paramquery] (reduce pmapping [] paramkeys)))
    (throw (RuntimeException. (str "Query " name " " querykey " was not found.")))))

(defn update-entity-with-version [{table :tablename} entity]
  (let [version (:version entity)
        version (if (nil? version) 0 version)
        versionentity (assoc entity :version (+ version 1))
        updateresult (sql/update-values table ["id=? AND version=?" (:id entity) (:version entity)] versionentity)]
    (if (= (first updateresult) 1)
      versionentity
      (not (sql/set-rollback-only)))))
