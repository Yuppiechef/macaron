(defproject com.yuppiechef/macaron "1.0.0-SNAPSHOT"
  :description "A declarative entity abstraction library for clojure."
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/java.jdbc "0.1.0"]
                 [swank-clojure "1.4.2"]
                 [log4j "1.2.16" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [org.slf4j/slf4j-log4j12 "1.6.4"]
                 [mysql/mysql-connector-java "5.1.18"]]
  :profiles {:dev {:plugins [[lein-midje "2.0.0-SNAPSHOT"]
                             [lein-swank "1.4.4"]]}})
