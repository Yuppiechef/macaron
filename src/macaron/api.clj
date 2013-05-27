(ns macaron.api
  (:require [clojure.java.jdbc :as sql]))

(defn new-entity
  "Create a new entity hashmap, stripping out all keys not present"
  [{fields :fields} & [valuemap]]
  (select-keys (or valuemap {}) (map first fields)))

(defn update-entity-with-version [{table :tablename} entity]
  (let [version (:version entity)
        version (if (nil? version) 0 version)
        versionentity (assoc entity :version (+ version 1))
        updateresult (sql/update-values table ["id=? AND version=?" (:id entity) (:version entity)] versionentity)]
    (if (= (first updateresult) 1)
      versionentity
      (not (sql/set-rollback-only)))))

(defn save-entity [{table :tablename :as entdef} entity]
  (if (and (contains? entity :id) (:id entity))
    (if (contains? entity :version)
      (update-entity-with-version entdef entity)
      (do (sql/update-values table ["id=?" (:id entity)] entity) entity))
    (assoc entity :id (:generated_key (sql/insert-record table entity)))))

(defn save-entities [table entities]
  (doall (for [entity entities]
           (save-entity table entity))))

(defn find-by
  [{tablename :tablename} idcol value]
  (let [result (sql/with-query-results rs
                 [(format "SELECT * FROM %s WHERE %s = ?" tablename (sql/as-identifier idcol)) value]
                 (into {} rs))]
    (if (empty? result)
      nil result)))

(defn delete-by
  "Delete an entity using the specified column provided"
  [{table :tablename} col value]
  (debug "Deleting data in table: " table " column: " col " value: " value)
  (sql/delete-rows table [(format "%s = ?" (sql/as-identifier col)) value]))

(defn list-entities
  "Get a simple list of entities from the database."
  [{table :tablename}]
  (sql/with-query-results rs [(format "SELECT * FROM %s" table)] (apply list rs)))

(defn list-query
  [entdef querykey & [param-map]]
  (let [sql-params (get-named-query entdef querykey param-map)]
     (trace sql-params)
     (sql/with-query-results rs sql-params
       rs)))

(defn find-or-create [entdef idcol entity]
  {:pre (map? entity)}
  (let [id (get entity idcol :not-found)]
    (cond
     (= :not-found id) (throw (RuntimeException. (str "Id column " idcol " not found on entity " entity)))
     :else
     (let [ent (find-by entdef idcol id)]
       (if (empty? ent)
         (save-entity entdef entity)
         ent)))))


(defn enum-from-code [entdef fldkey code]
  (let [enum (get-in entdef [:fields fldkey 2] :unknown)
        filterfn (partial filter #(= fldkey (keyword (first %))))
        entriesfn #(nth % 2)
        enumfld (-> entdef :fields filterfn first entriesfn eval eval)
        enum (get enumfld code :unknown)]
    (case enum
      :unknown (do (warn "Unknown enum value" code "for enum" entkey fldkey "in array" enumfld) 0)
      enum)))
