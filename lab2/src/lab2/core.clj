(ns lab2.core
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(defn GenerateSequenceDumbMethod
  [x y z]
  (if (< y (- z 1)) (let [value (* -1 (* 2 (peek x)))
                     counter (+ y 1)]
                    (GenerateSequenceDumbMethod (conj x value) counter z)
                 )
               (println x)))

(GenerateSequenceDumbMethod [1] 0 4)

