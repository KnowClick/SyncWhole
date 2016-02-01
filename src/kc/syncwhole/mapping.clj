(ns kc.syncwhole.mapping
  "functions for working with 'mappings' -
  the configuration format for describing how data should be moved."
  (:require [taoensso.timbre :as log]
            [hikari-cp.core :as conn-pool]
            [kc.syncwhole.vendor :as v]
            [kc.syncwhole.jdbc :as sw-jdbc]))


;;; ---- utilities ------------------------------------------------

(defn keywordize-table-name
  "Given a table name and an optional schema, return a keyword
  formed from the concatenation of those two values"
  ([table] (keywordize-table-name table nil))
  ([table schema]
   (keyword (if-not (nil? schema)
              (str (name schema) "." (name table))
              (name table)))))

;;; ---- fns that operate on mappings ------------------------------------------

(defn remove-useless-keys [m]
  (let [m (if (= (:insert m) :none)
            (dissoc m :insert)
            m)
        m (if (= (:update m) :none)
            (dissoc m :update)
            m)]
    m))

(defn add-full-table-names [m]
  (let [f (fn [{:keys [name schema] :as s}]
            (assoc s :full-table-name (keywordize-table-name name schema)))]
    (-> m
        (update-in [:source :table] f)
        (update-in [:target :table] f))))

(defn add-quoting [m]
  (let [f (fn [s]
            (assoc s :quoting (v/quoting-style (:db s))))]
    (-> m
        (update :source f)
        (update :target f))))

(defn change-db-keys [m]
  (let [f (fn [s] (-> s
                      (assoc :db-conf (:db s))
                      (dissoc :db)))]
    (-> m
        (update :source f)
        (update :target f))))

(defn remove-vendor [m]
  (let [f (fn [m]
            (if-let [v (:vendor m)]
              (-> m
                  (dissoc :vendor)
                  (assoc :adapter v))
              m))]
    (-> m
        (update-in [:source :db] f)
        (update-in [:target :db] f))))

(defn add-stateful-resources [m]
  (let [f (fn [{:keys [db-conf] :as s}]
            (assoc s
                   :db-spec {:datasource (sw-jdbc/make-datasource db-conf)}))]
    (-> m
        (update :source f)
        (update :target f))))

(defn stop-stateful-resources [m]
  (sw-jdbc/close-datasource (-> m :source :db-conf))
  (sw-jdbc/close-datasource (-> m :target :db-conf)))
