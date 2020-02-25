(ns lab3.core
  (:gen-class)
  (:use [clojure.data.csv]
        [clojure.data.json]))

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.data.json :as js])

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

; ========= First task - parsing the first csv files ==========

(def map_zal-skl9 (rest (vec (with-open [reader (io/reader "../data files/map_zal-skl9.csv")]
  ; due to the fact that read-csv produces lazy-seq,
  ; we use the 'doall' function that forces all lazy-seq
  (doall
    (csv/read-csv reader))))))

(def mp-posts_full (with-open [reader (io/reader "../data files/mp-posts_full.csv")]
                    (doall
                      (csv/read-csv reader))))

(def mps-declarations_rada (vec (with-open [reader (io/reader "../data files/mps-declarations_rada.json")]
  (vector
   (doall
    (js/read reader))))))

(vec
  (vector
     (clojure.string/split
       (line-seq
            (io/reader "../data files/plenary_vote_results-skl9.tsv"))
       #"\\"
        )))