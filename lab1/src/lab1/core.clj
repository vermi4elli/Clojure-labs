(ns lab1.core
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  )

; First task's unnamed function
((fn [x y z] (list
               (first x)
               (first y)
               (first z)))
 '(V B N J H)
 '((Y U I) (H J K) (8) 78)
 '(df FG HJ K L (O 0 9)))

; Second task's function
(defn CreateNewList
  [x y z]
  (list (first (rest (rest x)))
        (first (rest (rest (rest y))))
        (first (rest (rest (rest (rest (rest z)))))))
  )

; Just calling the second task's functions
(CreateNewList '(V B N J H) '((Y U I) (H J K) (8) 78) '(df FG HJ K L (O 0 9)))

;Third task's function
(defn EvalSets
  [x y]
  (clojure.set/intersection (clojure.set/difference (set x) (set y))
                            (clojure.set/difference (clojure.set/union (set x) (set y))
                                                     (clojure.set/intersection (set x) (set y)))))

; Just calling the third task's functions
(EvalSets '(V B N J H) '((Y U I) (H J K) (8) 78))