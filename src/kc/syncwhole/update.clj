(ns kc.syncwhole.update
  (:require [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as log]
            [honeysql.core :as h]
            [honeysql.helpers :as hh]
            [kc.syncwhole.vendor :as v]
            [kc.syncwhole.jdbc :as sw-jdbc]
            [kc.syncwhole.mapping :as sw-mapping]))

(defn update-fetched
  [mapping row target-db-spec target-table target-quoting]
  (let [target-pk (-> mapping :target :table :column/pk)
        update-query (-> (hh/update target-table)
                         ;; don't include the columns that make up the pk
                         ;; in the update, not because that will necessarily do
                         ;; anything bad (because they're the same value),
                         ;; but because it makes more
                         ;; sense to leave those columns be.
                         (hh/sset (dissoc row target-pk))
                         ;; scope the update to only those rows matching the pk.
                         ;; TODO: pks can be multiple columns and this rewrite
                         ;; fails to consider that.
                         (hh/where [:= target-pk (get row target-pk)]))
        update-query (h/format update-query :quoting target-quoting)]
    (try (jdbc/execute! target-db-spec update-query)
         (catch Exception e
           (log/warn
            (sw-mapping/desc mapping) " - "
            (str "Attempt to update record " row
                 " failed. Exception: " (.getMessage e)))
           (throw e)))))

(defn fetch-and-update
  [select-query mapping]
  (log/info "Performing fetch and update for source table"
            (-> mapping :source :table :full-table-name)
            "and target table"
            (-> mapping :target :table :full-table-name)
            "using select query: " select-query)
  (let [row-transformer (-> mapping :update :row-fn)
        target-db-spec (-> mapping :target :db-spec)
        target-table (-> mapping :target :table :full-table-name)
        target-quoting (v/quoting-style (-> mapping :target :db-conf))
        result (sw-jdbc/batched-fetch
                :query select-query
                :db-spec (-> mapping :source :db-spec)
                :fetch-size 100
                :result-set-fn sw-jdbc/count-result-set-records
                :row-fn (fn [r]
                          (if-let [r (if row-transformer (row-transformer r) r)]
                            (update-fetched
                             mapping
                             r
                             target-db-spec
                             target-table
                             target-quoting))))]
    (log/info "Fetched and updated" result "rows in "
              (-> mapping :target :table :full-table-name))))

(defn timestamps-update-query [mapping source-maxes target-maxes]
  (let [source (-> mapping :source)
        source-pk-col (-> source :table :column/pk)
        source-created-col (-> source :table :column/created)
        source-updated-col (-> source :table :column/updated)
        source-created-precision (-> source :table :column.created/precision)
        source-updated-precision (-> source :table :column.updated/precision)
        source-db-vendor (-> source :db-conf :adapter)
        target (-> mapping :target)
        target-pk-col (-> target :table :column/pk)
        target-created-precision (-> target :table :column.created/precision)
        target-updated-precision (-> target :table :column.updated/precision)
        [clj-created-trunc sql-created-trunc]
        (if (< target-created-precision source-created-precision)
          (let [p target-created-precision]
            [(v/clj-timestamp-truncator p)
             (v/sql-timestamp-truncator source-db-vendor p)])
          [identity identity])
        [clj-updated-trunc sql-updated-trunc]
        (if (< target-updated-precision source-updated-precision)
          (let [p target-updated-precision]
            [(v/clj-timestamp-truncator p)
             (v/sql-timestamp-truncator source-db-vendor p)])
          [identity identity])
        q (-> (hh/select :*)
              (hh/from (-> source :table :full-table-name))
              (hh/where
               [:and
                [:<= source-pk-col (:pk target-maxes)]
                [:<=
                 (sql-created-trunc source-created-col)
                 (:created target-maxes)]
                [:<=
                 (:updated target-maxes)
                 (sql-updated-trunc source-updated-col)
                 (if-let [d (:updated source-maxes)]
                   (clj-updated-trunc d))]])
              (h/format :quoting (-> source :quoting)))]
    q))

(defn get-update-conf-type [mapping]
  (or (-> mapping :update :when :compare)
      (-> mapping :update)))

(defmulti update-fn
  "Return a function for updating the target table based on the
  configuration in the mapping"
  {:arglists '([mapping source-maxes target-maxes])}
  (fn [mapping _ _] (get-update-conf-type mapping)))

(defmethod update-fn :default [mapping _ _]
  (throw (UnsupportedOperationException.
          (str "Update " (get-update-conf-type mapping) " not implemented.\n"
               "You can implement it by implementing a method of the multimethod "
               "kc.syncwhole.update/update-fn"))))

(defn timestamps-update-fn
  "update target from source where
  source pk <= max target pk
  && source created <= max target created
  && max target updated <= source updated <= max source updated
  if source created/updated precision is greater than target precision,
  truncation is required, so it looks more like:
  source pk <= max target pk
  && truncate(source created) <= max target created
  && max target updated <= truncate(source updated) <= truncate(max source updated)"
  [mapping source-maxes target-maxes]
  (fn []
    (let [q (timestamps-update-query mapping source-maxes target-maxes)]
      (fetch-and-update q mapping))))

(defmethod update-fn :timestamps [mapping source-maxes target-maxes]
  (timestamps-update-fn mapping source-maxes target-maxes))
