(ns lab1.functions)

(defn CreateNewList
  [x y z]
  (list (first (rest (rest x))) (first (rest (rest (rest y)))) (first (rest (rest (rest (rest (rest z)))))))
  )

(CreateNewList '(V B N J H) '((Y U I) (H J K) (8) 78) '(df FG HJ K L (O 0 9)))

((fn [x y z] (list (first x) (first y) (first z))) '(V B N J H) '((Y U I) (H J K) (8) 78) '(df FG HJ K L (O 0 9)))

(defn EvalSets
  [x y]
  (clojure.set/intersection (clojure.set/difference (set x) (set y))
                            (clojure.set/difference (clojure.set/union (set x) (set y))
                                                    (clojure.set/intersection (set x) (set y)))))

(EvalSets '(V B N J H) '((Y U I) (H J K) (8) 78))