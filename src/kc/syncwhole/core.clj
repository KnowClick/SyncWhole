(ns kc.syncwhole.core
  "Tools for configuration-driven change data capture"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.pprint]
            [clojure.string :as string]
            [taoensso.timbre :as log]
            [honeysql.core :as h]
            [honeysql.helpers :as hh]
            [clj-time.jdbc]
            [clj-time.core :as clj-time]
            [hikari-cp.core :as conn-pool]
            [kc.syncwhole.vendor :as v]
            [kc.syncwhole.mapping :as sw-mapping]
            [kc.syncwhole.jdbc :as sw-jdbc]
            [kc.syncwhole.insert :as sw-insert]
            [kc.syncwhole.update :as sw-update]))

;;; ---- utilities -------------------------------------------------------------

(defn pprint [& more]
  (with-out-str (apply clojure.pprint/pprint more)))

;;; ---- Environment Validation ------------------------------------------------

(defn verify-time-zone
  "Make sure the VM's time zone is UTC or an equivalent.
  TODO: figure out how to make JDBC not screw up timestamps when the
  VM isn't using UTC"
  []
  (let [tz-ids (into [] (java.util.TimeZone/getAvailableIDs))
        acceptable-ids (->> tz-ids
                            (map
                             (fn [id]
                               [id (.getRawOffset
                                    (java.util.TimeZone/getTimeZone id))]))
                            (filter
                             (fn [[id offset]] (= offset 0)))
                            (map first)
                            set)
        tz (System/getProperty "user.timezone")]
    (when-not (some acceptable-ids [tz])
      (throw (IllegalStateException.
              (str
               "System must be in UTC or equivalent time zone. You're in " tz))))))

;;; ---- input validation ------------------------------------------------------

(defn validate-insert-and-update-conf [mapping]
  (let [{:keys [insert update]} mapping]
    (when-let [insert-comparison (some-> insert :when :compare)]
      (when (= insert-comparison :pk)
        (let [source-pk (some-> mapping :source :table :column/pk)
              target-pk (some-> mapping :target :table :column/pk)]
          (if-not (and source-pk target-pk)
            (throw (IllegalArgumentException.
                    (str "When inserting based on primary key, "
                         "both source and target need a :column/pk key "
                         "naming a column existing in the table.\n"
                         (pprint mapping))))))))
    (when-let [update-comparison (some-> update :when :compare)]
      (when (= update-comparison :timestamps)
        (if-not (and (some-> mapping :source :table :column/pk)
                     (some-> mapping :source :table :column/created)
                     (some-> mapping :source :table :column/updated)
                     (some-> mapping :target :table :column/pk)
                     (some-> mapping :target :table :column/created)
                     (some-> mapping :target :table :column/updated))
          (throw (IllegalArgumentException.
                  (str "When updating based on timestamps, "
                       "both source and target need :column/pk, "
                       ":column/created and :column/updated keys.\n"
                       (pprint mapping)))))))))

;;; ---- initial data gathering ------------------------------------------------

(defn get-timestamp-precisions [mapping]
  (let [source-db-spec (-> mapping :source :db-spec)
        source-db-conf (-> mapping :source :db-conf)
        target-db-spec (-> mapping :target :db-spec)
        target-db-conf (-> mapping :target :db-conf)
        source #(v/timestamp-precision
                 source-db-conf
                 (-> mapping :source :table)
                 %)
        target #(v/timestamp-precision
                 target-db-conf
                 (-> mapping :target :table)
                 %)]
    {:source/created (source (-> mapping :source :table :column/created))
     :source/updated (source (-> mapping :source :table :column/updated))
     :target/created (target (-> mapping :target :table :column/created))
     :target/updated (target (-> mapping :target :table :column/updated))}))

(defn get-maxes [conf]
  (let [col-keys #{:column/pk :column/created :column/updated}
        cols (->> conf
                  :table
                  keys
                  (filter #(some col-keys [%]))
                  (map (fn [k] [k (-> conf :table k)]))
                  (into {})
                  (map (fn [[col-purpose col-name]]
                         [(h/call :max col-name) col-purpose])))
        q (-> (apply hh/select cols)
              (hh/from (-> conf :table :full-table-name))
              (h/format :quoting (-> conf :quoting)))]
    (jdbc/query (-> conf :db-spec) q :result-set-fn first)))

;;; ---- do things! ------------------------------------------------------------

(defn move-mapping! [mapping]
  (let [source-maxes (get-maxes (-> mapping :source))
        target-maxes (get-maxes (-> mapping :target))
        insert-fn (sw-insert/insert-fn mapping source-maxes target-maxes)
        update-fn (sw-update/update-fn mapping source-maxes target-maxes)]
    (dorun (->> [insert-fn update-fn]
                (filter fn?)
                (pmap
                 (fn [f]
                   (f)))))))

(defn expand-mapping [item conf]
  (-> item
      (update-in [:source :db] (fn [k] (-> conf :databases k)))
      (update-in [:target :db] (fn [k] (-> conf :databases k)))
      (update-in [:source :table] (fn [k] (-> conf :tables k)))
      (update-in [:target :table] (fn [k] (-> conf :tables k)))
      sw-mapping/remove-vendor
      sw-mapping/add-quoting
      sw-mapping/change-db-keys
      sw-mapping/add-full-table-names
      sw-mapping/remove-useless-keys))

(defn move-mapping!* [item conf]
  (let [mapping (expand-mapping item conf)]
    (validate-insert-and-update-conf mapping)
    (let [timestamp-precisions (if (= :timestamps
                                      (-> mapping :update :when :compare))
                                 (get-timestamp-precisions mapping)
                                 nil)
          mapping (-> mapping
                      (assoc-in [:source :table :column.created/precision]
                                (:source/created timestamp-precisions))
                      (assoc-in [:source :table :column.updated/precision]
                                (:source/updated timestamp-precisions))
                      (assoc-in [:target :table :column.created/precision]
                                (:target/created timestamp-precisions))
                      (assoc-in [:target :table :column.updated/precision]
                                (:target/updated timestamp-precisions))
                      sw-mapping/add-stateful-resources)]
      (try
        (move-mapping! mapping)
        (finally (do
                   (log/debug "Closing datasources...")
                   (sw-mapping/stop-stateful-resources mapping)
                   (log/debug "Closed datasources...")))))))

(defn move-sequence! [sequence conf]
  (doseq [s sequence]
    (move-mapping!* s conf)))

(defn move-group! [item conf]
  (cond
    (map? item) (move-mapping!* item conf)
    (map? (first item)) (move-sequence! item conf)
    :else (throw (IllegalArgumentException. ":sequence item not recognized:" item))))

(defn move!
  "Move some number of tables based on the supplied configuration."
  [conf]
  (verify-time-zone)
  ;; :sequences is a vector of maps and vectors of maps.
  ;;   all maps can start running immediately.
  ;;   vectors of maps run sequentially; the first of each can start immediately.
  ;;  therefore, every top level thing in :sequences can be run in parallel
  (dorun (pmap #(move-group! % conf) (:sequences conf)))
  ;; clean up connection pools.
  ;; only needed in dev because the alternative is to keep creating new
  ;; connection pools until a db server runs out of available connections.
  (sw-jdbc/cleanup-datasources))
