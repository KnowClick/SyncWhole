(ns kc.syncwhole.jdbc
  "jdbc utilities"
  (:require [clojure.java.jdbc :as jdbc]
            [hikari-cp.core :as conn-pool]
            [kc.syncwhole.vendor :as v]))

(defn set-parameters
  "Add the parameters to the given statement."
  [stmt params]
  (dorun (map-indexed (fn [ix value]
                        (jdbc/set-parameter value stmt (inc ix)))
                      params)))

(defn batched-fetch
  "A more configurable version of jdbc/query"
  [& {:keys [query db-spec fetch-size concurrency row-fn as-arrays? result-set-fn]
      :or {fetch-size 50
           concurrency :read-only
           row-fn identity
           as-arrays? false
           result-set-fn doall}}]
  (let [stmt-query (if (string? query) query (first query))
        stmt-params (if (= stmt-query query) [] (rest query))]
    (with-open [^java.sql.Connection conn (jdbc/get-connection db-spec)]
      (.setAutoCommit conn false)
      (with-open [^java.sql.PreparedStatement statement (jdbc/prepare-statement
                                                         conn
                                                         stmt-query
                                                         :fetch-size fetch-size
                                                         :concurrency concurrency)]
        ((or (:set-parameters db-spec) set-parameters) statement stmt-params)
        (let [results (jdbc/query conn [statement]
                                  :row-fn row-fn
                                  :result-set-fn result-set-fn
                                  :as-arrays? as-arrays?)]
          results)))))

(defn count-result-set-records
  "Use as a jdbc :result-set-fn
  Just returns the number of results in a result set"
  [rs]
  (reduce (fn [total row-map]
            (+ total 1))
          0 rs))

;;; ---- confusing and probably wrong connection pool deduplication ------------

(def datasources (agent {}))

(defn make-datasource [db-conf]
  (send-off datasources
            (fn [datasources]
              (if-let [ds-conf (get datasources db-conf)]
                (do
                  (update-in datasources [db-conf :count] inc))
                (let [ds (conn-pool/make-datasource
                          (assoc db-conf
                                 :configure (fn [conf]
                                              (.setConnectionInitSql
                                               conf
                                               (v/connection-init-sql
                                                db-conf)))))
                      ds-conf {:count 1 :ds ds}]
                  (assoc datasources db-conf ds-conf)))))
  (await datasources)
  (:ds (get @datasources db-conf)))

(defn close-datasource [db-conf]
  (send-off
   datasources
   (fn [datasources]
     (if-let [{:keys [count ds]} (get datasources db-conf)]
       (if (= count 1)
         (do
           (conn-pool/close-datasource ds)
           (dissoc datasources db-conf))
         (do
           (update-in datasources [db-conf :count] dec)))
       (throw (Exception. "Cannot close nonexistent datasource"))))))

(defn cleanup-datasources []
  (dorun (map (comp conn-pool/close-datasource :ds) (vals @datasources)))
  (send datasources (constantly {})))
