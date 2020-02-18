(ns lab2.core
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

; The first task of the lab2
; This function get three arguments: x -> [1]   - vector with an element 1
;                                    y -> 0     - temp counter, will be used
;                                                 to check if we reached the limit
;                                    z -> limit - the amount of elements needed
(defn GenerateSequenceDumbMethod
  [x y z]
  (if (< y (- z 1)) (let [value (* -1 (* 2 (peek x)))
                          counter (+ y 1)]
                          (GenerateSequenceDumbMethod (conj x value) counter z))
                    (println x)))

; Just calling the function
(GenerateSequenceDumbMethod [1] 0 4)

; The second task of the lab2
; Creating functions: swap      - to swap two elements of the vector
;                                 based on their INDEXES
;                     partition - to get index of the index in the vector
;                                 which will cut the vector in two parts
;                     quicksort - the sorting algorithm itself
(defn swap [v i1 i2]
  (assoc v i2 (v i1) i1 (v i2)))

(swap '[1 3 5 67] 0 2)

(defn partition
  [])
