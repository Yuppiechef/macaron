(ns macaron.impl.core)

(defonce entitydefs (atom {}))

(defn process-field [[nm tp & opts]]
  (let [nm (keyword (name nm))]
    (cond
     (= :link tp) [nm [tp (first opts)]]
     :else [nm (apply vector tp opts)])))

(defn process-index [[nm tp & opts]]
  [(keyword nm) [tp (vec (map #(update-in % [0] keyword) (first opts)))]])

(defn merge-entitydef-super [base super]
  (merge-with into (dissoc super :tablename :name) base))

(defn extend-entdef [entdef]
  (let [extended (:extends entdef)]
    (if extended
      (reduce merge-entitydef-super entdef extended)
      entdef)))

(defn entity-register! [{name :name :as entitydef}]
  (swap! entitydefs assoc (keyword name) entitydef))
