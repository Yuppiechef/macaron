(ns macaron.core
  (:use [clojure.tools.logging])
  (:require [clojure.java.jdbc :as sql])
  (:require [clojure.string :as string])
  (:require [clojure.set :as set]))

(defonce entitydefs (atom {}))

(def coltypes
  #{:version :currency :auto-index :uuid :unique-uuid :full-varchar
    :password :enum :email :ipaddress :link :boolean :date})

(def default-indextypes
  {:uuid :unique
   :text :fulltext
   :mediumtext :fulltext
   :longtext :fulltext})

(defn process-field [[nm tp & opts]]
  (let [nm (keyword (name nm))]
    (cond
     (= :link tp) [nm [tp (name (first opts))]]
     :else [nm (apply vector tp opts)])))

(defn process-index [[nm tp & opts]]
  [(keyword nm) [tp (map #(update-in % [0] keyword) (first opts))]])

(defn convert-to-def [nm opts]
  (let [mapped (apply merge (map #(hash-map (keyword (name (first %))) (vec (rest %))) opts))]
    (assoc mapped
      :name (name nm)
      :extends (into [] (map #(name %) (:extends mapped)))
      :fields (into {} (map process-field (:fields mapped)))
      :manytomany (into [] (map #(name %) (:manytomany mapped)))
      :post-create-fn (apply vector (:post-create-fn mapped))
      :queries (apply hash-map (:queries mapped))
      :indices (map process-index (:indices mapped))
      :tablename (if (:tablename mapped) (first (:tablename mapped)) nm))))

(defn merge-entitydef-super [base supernm]
  (if-let [super ((keyword supernm) @entitydefs)]
    (merge-with into (dissoc super :tablename :name) base)
    base))

(defn convert-with-extends [nm opts]
  (let [converted (convert-to-def nm opts)
        extended (:extends converted)]
    (if extended
      (reduce merge-entitydef-super converted extended)
      converted)))

(defn entity-register! [{name :name :as entitydef}]
  (swap! entitydefs assoc name entitydef))

(defn tablename [nm]
  {:tablename nm})

(defn extends [& entities]
  {:extends (map :name entities)})

(defn queries [& name-query-pairs]
  {:queries (apply hash-map name-query-pairs)})

(defmacro fields [& fielddefs]
  (let [defs (into {} (map process-field fielddefs))]
    `{:fields ~defs}))

(defmacro indices [& indices]
  (let [defs (into {} (map process-index indices))]
    (println defs)))

(defmacro manytomany [& links]
  `{:manytomany ~(vec links)})

(defn defmtmlink
  "Create basic many to many linking entity definition."
  [entname link-tablename left-entname right-entname]
  (println entname link-tablename left-entname right-entname)
  `(defentity ~entname
     (tablename ~(str link-tablename))
     (fields
      [~(symbol (str left-entname "_id")) :link ~(symbol left-entname)]
      [~(symbol (str right-entname "_id")) :link ~(symbol right-entname)])))

(defn many-to-many-definition [{entname :name mtmlinks :manytomany :as ent}]
  (when (> (count mtmlinks) 0)
    (let [links (for [{lname :name} mtmlinks
                      :let [link-entname (str entname "-" lname)
                            link-tablename (str entname "_" lname)
                            link-key (keyword link-tablen8ame)]]
                  (defmtmlink link-entname link-tablename entname lname))]
      (eval `(do ~@links)))))

(defmacro defentity-template [nm & opts]
  (let [entitydef (dissoc (convert-to-def nm opts) :tablename)]
    (println entitydef)
    `(do
       (def ~nm ~entitydef)
       (entity-register! ~nm))))

(defmacro defentity [nm & body]
  (let [mapped {:name (name nm)}]
    `(do
       (def ~nm (merge {:name ~(keyword nm)}~@body))
       (entity-register! ~nm)
       (many-to-many-definition ~nm))))
