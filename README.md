# SyncWhole

Move data between jdbc compliant databases.

## Usage 

`[kc/syncwhole "0.0.3"]`

SyncWhole can be used as a library or as an independent application. 

### As a library

Call `kc.syncwhole.core/move!` with a configuration map.

When using as a library, the configuration map can accept additional keys at all :insert and :update configuration locations. You may provide a :row-fn to any update or insert to transform each record from the source before inserting it in target. You can use this to add or remove columns, etc. Generally, you'll want to supply the same :row-fn to inserts and updates on the same table.

### As an application 

Run the application, passing the path to a file containing a configuration map:

```
  java -jar kc-syncwhole-standalone.jar /path/to/file.edn 
```

### Notes 
- When moving data from a table with higher precision timestamps to one with lower precision timestamps, updates will re-update the most recent records in the target table on each sync. This doesn't upset data integrity unless you have an additional `updated` column in the target table.
- This library does not handle recreating the schema from the source database in the target database. We currently handle this but just running all database migrations in both the source and target databases.

## Configuration Maps:
A map with three required keys: :databases, :tables, :sequences. :sequences is a vector of maps or other vectors. All top level items are run in parallel. Maps are treated as source table to target table mappings and vectors are treated as sequences of the same, to be run sequentially. 

## Example

```clojure
(def conf
  {:databases
   {:kcadmin {:server-name "127.0.0.1"
              :database-name "admin"
              :vendor "postgresql"
              :username "foo"
              :password "bar"}
    :reporting {:server-name "127.0.0.1"
                :database-name "admin"
                :vendor "mysql"
                :username "qux"
                :password "baz"}}
   :tables {:source/test {:schema :public
                          :name :sync_test
                          :column/pk :id
                          :column/created :created_at
                          :column/updated :updated_at}
            :target/test {:schema :kcadmin
                          :name :sync_test
                          :column/pk :id
                          :column/created :created_at
                          :column/updated :updated_at}
            :source/test2 {:schema :public
                           :name :sync_test2
                           :column/pk :id
                           :column/created :created_at
                           :column/updated :updated_at}
            :target/test2 {:schema :kcadmin
                           :name :sync_test2
                           :column/pk :id
                           :column/created :created_at
                           :column/updated :updated_at}}
   :sequences
   [{:source {:db :kcadmin :table :source/test}
     :target {:db :reporting :table :target/test}
     :insert {:when {:compare :pk}
              ;; you can only use a :row-fn when using syncwhole as a library:
              :row-fn (fn [r] (update r :value #(string/replace % "\n" " ")))}
     :update {:when {:compare :timestamps}}}
    {:source {:db :kcadmin :table :source/test2}
     :target {:db :reporting :table :target/test2}
     :insert {:when {:compare :pk}}
     :update {:when {:compare :timestamps}}}]})

(kc.syncwhole.core/move! conf)
```

## Implementation Notes 

### `db-conf` vs `db-spec` 
A `db-conf` map describes the things needed to connect to a database. `db-conf`s should not be passed to jdbc functions. `db-spec` maps are derived from `db-conf` maps and should be passed to jdbc functions.

### Extensibility
Adding support for new database vendors or new insert or update logic is handled by extending several multimethods. 
Currently, the vendors postgresql and mysql, inserting by primary key comparisons, and updating by timestamp comparisons are supported.
Have a look in kc.syncwhole.vendor to see the multimethods to extend for a new vendor.
Look in kc.syncwhole.insert and kc.syncwhole.update for the multimethods to extend for new insert or update logic. 

## License

Distributed under the Eclipse Public License, the same as Clojure.

## TODO
- add schema validation 
- insert/update relations using foreign keys
