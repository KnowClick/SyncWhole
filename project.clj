(defproject syncwhole "0.0.1"
  :description "Move data between jdbc compliant databases"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [honeysql "0.6.2"]
                 [com.taoensso/timbre "4.2.1"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 [mysql/mysql-connector-java "5.1.38"]
                 [clj-time "0.10.0"]
                 [hikari-cp-java6 "1.5.0"]]
  :jvm-opts ["-Duser.timezone=GMT"]
  :repl-options {:init-ns user}
  :profiles
  {:dev {:main kc.syncwhole.core}
   :prd [:base :system
         {:aot :all
          :main kc.syncwhole.app
          :uberjar-name "kc-syncwhole-standalone.jar"}]})
