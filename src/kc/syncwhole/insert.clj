(ns kc.syncwhole.insert
  (:require [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as log]
            [honeysql.core :as h]
            [honeysql.helpers :as hh]
            [kc.syncwhole.vendor :as v]
            [kc.syncwhole.jdbc :as sw-jdbc]))

(defn insert-fetched
  [mapping row target-db-spec target-table target-quoting]
  (try (jdbc/insert! target-db-spec
                     target-table
                     row
                     :entities target-quoting)
       (catch Exception e
         (log/warn
          (str "Attempt to insert record " row
               " into table " target-table
               " failed. Exception: " (.getMessage e)))
         (throw e))))

(defn fetch-and-insert
  [select-query mapping]
  (log/info "Performing fetch and insert for source table"
            (-> mapping :source :table :full-table-name)
            "and target table"
            (-> mapping :target :table :full-table-name)
            "using select query: " select-query)
  (let [row-transformer (-> mapping :insert :row-fn)
        target-db-spec (-> mapping :target :db-spec)
        target-table (-> mapping :target :table :full-table-name)
        target-quoting (v/quoting-fn (-> mapping :target :db-conf))
        result (sw-jdbc/batched-fetch
                :query select-query
                :db-spec (-> mapping :source :db-spec)
                :fetch-size 100
                :result-set-fn sw-jdbc/count-result-set-records
                :row-fn (fn [r]
                          (if-let [r (if row-transformer (row-transformer r) r)]
                            (insert-fetched
                             mapping
                             r
                             target-db-spec
                             target-table
                             target-quoting))))]
    (log/info "Fetched and inserted" result "rows into "
              (-> mapping :target :table :full-table-name))))

(defn get-insert-conf-type [mapping]
  (or (-> mapping :insert :when :compare)
      (-> mapping :insert)))

(defmulti insert-fn
  "Return a function for inserting to the target based on the configuration
  in the mapping."
  {:arglists '([mapping source-maxes target-maxes])}
  (fn [mapping _ _] (get-insert-conf-type mapping)))

(defmethod insert-fn :default [mapping _ _]
  (throw (UnsupportedOperationException.
          (str "Insert " (get-insert-conf-type mapping) " not implemented.\n"
               "You can implement it by implementing a method of the multimethod "
               "kc.syncwhole.insert/insert-fn"))))

(defmethod insert-fn :pk [mapping source-maxes target-maxes]
  (fn []
    ;; insert into target from source
    ;; where max target pk < source.pk <= max source pk
    (let [source-pk-col (-> mapping :source :table :column/pk)
          q (-> (hh/select :*)
                (hh/from (-> mapping :source :table :full-table-name))
                (hh/where [:<= source-pk-col (:pk source-maxes)]))
          q (if-let [target-pk (:pk target-maxes)]
              (hh/merge-where q [:< target-pk source-pk-col])
              q)
          q (h/format q :quoting (-> mapping :source :quoting))]
      (fetch-and-insert q mapping))))
