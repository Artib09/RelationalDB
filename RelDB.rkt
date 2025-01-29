#lang racket
;Arti Brzozowski

(provide (struct-out column-info)
         (struct-out table)
         (struct-out and-f)
         (struct-out or-f)
         (struct-out not-f)
         (struct-out eq-f)
         (struct-out eq2-f)
         (struct-out lt-f)
         table-insert
         table-project
         table-sort
         table-select
         table-rename
         table-cross-join
         table-natural-join)

(define-struct column-info (name type) #:transparent)
(define-struct table (schema rows) #:transparent)
(define (empty-table columns) (table columns '()))
(define (empty-table? tab) (null? (table-rows tab)))

;Procedure for converting schema to a list of column names or types
(define (schema->lst-names/types names? schema acc)
  (if (null? schema)
      (reverse acc)
      (schema->lst-names/types names?
                               (cdr schema)
                               (cons ((if names?
                                          column-info-name
                                          column-info-type) (car schema))
                                     acc))))

;Definitions unifying comparison names
(define number<? <)
(define (boolean<? x y) (< (if (equal? #f x) 0 1) (if (equal? #f y) 0 1)))

(define (without-last l) (reverse (cdr (reverse l))))

;Procedure for adding symbol to symbol
(define (symbol-append s to-app)
  (let ([ns (variable-reference->namespace (#%variable-reference))]
        [sym (string->symbol (string-append (symbol->string s) to-app))])
    (eval sym ns)))

;Name change
(define (table-rename col ncol tab) 
  (define (rename schema acc)
    (if (null? schema)
        (reverse acc)
        (rename (cdr schema)
                (if (equal? col (column-info-name (car schema)))
                    (cons (column-info ncol (column-info-type (car schema))) acc)
                    (cons (car schema) acc)))))
  (table (rename (table-schema tab) '()) (table-rows tab)))

;Insertion
(define (table-insert row tab)
  ;Procedure to check if newline types are correct
  (define (check-types acc row types)
    (if (or (null? row)(null? types))
        (if (and (null? row)(null? types))
            acc
            #f)
        (if ((symbol-append (column-info-type (car types)) "?") (car row))
            (check-types acc (cdr row) (cdr types))
            (check-types #f (cdr row) (cdr types)))))
  ;Inserting into table
  (if (check-types #t row (table-schema tab))
      (table (table-schema tab) (cons row (table-rows tab)))
      (error "row do not match table")))

;Projection
(define (table-project cols tab)
  ;Procedure that finds elements to remove and creates a new schema
  ;returns a pair - a new table schema and a list of indexes of elements to remove
  (define (to-remove schema cols lst counter newschema)
    (if (null? schema)
        (cons (reverse lst) (reverse newschema))
        (let ([check (list? (member (column-info-name (car schema))
                                    cols))])
          (to-remove (cdr schema)
                     cols
                     (if check lst (cons counter lst))
                     (+ counter 1)
                     (if check (cons (car schema) newschema) newschema)))))
  ;Procedure to remove values ​​from each row based on the list of indexes to remove
  (define (remove-from-row lst row res counter)
    (if (null? lst)
        (append (reverse res) row)
        (let ([check (equal? counter (car lst))])
          (remove-from-row (if check (cdr lst) lst)
                           (cdr row)
                           (if check res (cons (car row) res)) 
                           (+ counter 1)))))
  ;Main projection
  ;removes elements from each row
  (define (it-proj names-to-rem rows acc)
    (if (null? rows)
        (reverse acc)
        (it-proj names-to-rem
                 (cdr rows)
                 (cons (remove-from-row names-to-rem (car rows) '() 0) acc))))
  (let ([rem (to-remove (table-schema tab) cols '() 0 '())])
    (table (cdr rem) (it-proj (car rem) (table-rows tab) '()))))

;Cross join
(define (table-cross-join tab1 tab2)
  ;Procedure that connects one row to the others
  (define (cross-join-row row rows acc)
    (if (null? rows)
        acc
        (cross-join-row row (cdr rows) (cons (append row (car rows)) acc))))
  ;Procedure to perform a cross join
  (define (join rows1 rows2 acc)
    (if (null? rows1)
        acc
        (join (cdr rows1) rows2 (append (cross-join-row (car rows1) rows2 '()) acc))))
  ;Checking conditions for empty lists
  (cond [(empty-table? tab1) (table (append (table-schema tab1)
                                            (table-schema tab2))
                                    '())]
        [(empty-table? tab2) (table (append (table-schema tab1)
                                            (table-schema tab2))
                                    '())]
        [else (table (append (table-schema tab1)(table-schema tab2))
         (join (table-rows tab1) (table-rows tab2) '()))]))

;Natural join
(define (table-natural-join tab1 tab2)
  ;Procedure that removes elements of the second list from the first list
  (define (list-subtract xs ys)
    (if (empty? ys)
        xs
        (list-subtract (remove (first ys) xs) (rest ys))))
  ;Procedure finds columns which need to be renamed
  (define (find-to-rename schema-lst1 schema-lst2 acc)
    (if (null? schema-lst1)
        acc
        (find-to-rename (cdr schema-lst1)
                        schema-lst2
                        (if (list? (member (car schema-lst1) schema-lst2))
                            (cons (car schema-lst1) acc)
                            acc))))
  ;Procedure names all the columns which need to be changed
  (define (rename-all to-rename tab acc)
    (if (null? to-rename)
        (cons acc tab)
        (let ([sym (gensym (car to-rename))])
          (rename-all (cdr to-rename)
                      (table-rename (car to-rename) sym tab)
                      (cons (cons (car to-rename) sym) acc)))))
  ;Procedure to check if rows should be in a result of the merge
  (define (row-check names row renamed-lst acc)
    (if (null? renamed-lst)
        acc
        (let ([index1 (index-of names (caar renamed-lst))]
              [index2 (index-of names (cdar renamed-lst))])
          (row-check names row (cdr renamed-lst) (and (equal? (list-ref row index1)
                                                              (list-ref row index2))
                                                      acc)))))
  ;Procedure that "collects" rows with the same keys
  ;creates rows for the resulting table
  (define (join-rows names rows renamed-lst acc)
    (if (null? rows)
        acc
        (join-rows names (cdr rows) renamed-lst (if (row-check names (car rows) renamed-lst #t)
                                                    (cons (car rows) acc)
                                                    acc))))
  ;Main natural join
  (let* ([rename-res (rename-all (find-to-rename (schema->lst-names/types #t (table-schema tab1) '())
                                                (schema->lst-names/types #t (table-schema tab2) '())
                                                '())
                                tab2
                                '())]
         [cross-tab (table-cross-join tab1 (cdr rename-res))]
         [schema (table-schema cross-tab)]
         [schema-names (schema->lst-names/types #t schema '())]
         [renamed-pairs (car rename-res)])
    (if (empty-table? cross-tab)
        cross-tab
        (table-project (list-subtract schema-names (map (lambda (x) (cdr x)) renamed-pairs))
                       (table schema (join-rows schema-names (table-rows cross-tab) renamed-pairs '()))))))

;Sort
(define (table-sort cols tab)
  ;Sorting by one column
  (define (sort-by-col col tab)
    (let* ([schema (table-schema tab)]
           [rows (table-rows tab)]
           [index (index-of (schema->lst-names/types #t schema '()) col)]
           [col-num (if index index (error "no column with given name"))]
           [sort-type (symbol-append (column-info-type (list-ref schema col-num)) "<?")]
           [ns (variable-reference->namespace (#%variable-reference))])
      (sort rows #:key (lambda (l) (list-ref l col-num)) (eval sort-type ns))))
  ;Main sort
  (if (null? cols)
      tab
      (table-sort (without-last cols) (table (table-schema tab) (sort-by-col (last cols) tab)))))

;Selection
(define-struct and-f (l r))        ;conjunction of formulas l and r
(define-struct or-f (l r))         ;disjunction of formulas l and r
(define-struct not-f (e))          ;negation of the formula
(define-struct eq-f (name val))    ;name column value equal to val
(define-struct eq2-f (name name2)) ;column values ​​name and name2 equal
(define-struct lt-f (name val))    ;name column value less than val

(define (table-select form tab)
  ;Procedure for evaluating eq, eq2 and lt
  (define (eval-eq-lt schema rows index val eq? val? acc)
    (if (set-empty? rows)
        acc
        (let* ([set-elem (set-first rows)]
               [rest (set-remove rows set-elem)]
               [type (list-ref (schema->lst-names/types #f schema '()) index)])
          (if ((if eq? equal? (symbol-append type "<?")) (list-ref set-elem index)
                                                         (if val? val (list-ref set-elem val)))
              (eval-eq-lt schema rest index val eq? val? (set-add acc set-elem))
              (eval-eq-lt schema rest index val eq? val? acc)))))
  ;Procedure for evaluating formulas
  (define (eval-form schema rows-set f)
    (cond
      [(and-f? f) (set-intersect (eval-form schema rows-set (and-f-l f))
                                 (eval-form schema rows-set (and-f-r f)))]
      [(or-f? f) (set-union (eval-form schema rows-set (or-f-l f))
                            (eval-form schema rows-set (or-f-r f)))]
      [(not-f? f) (set-subtract rows-set (eval-form schema rows-set (not-f-e f)))]
      [(eq-f? f) (eval-eq-lt schema
                             rows-set
                             (index-of (schema->lst-names/types #t schema '()) (eq-f-name f))
                             (eq-f-val f)
                             #t
                             #t
                             (set))]
      [(eq2-f? f) (eval-eq-lt schema
                              rows-set
                              (index-of (schema->lst-names/types #t schema '()) (eq2-f-name f))
                              (index-of (schema->lst-names/types #t schema '()) (eq2-f-name2 f))
                              #t
                              #f
                              (set))]
      [(lt-f? f) (eval-eq-lt schema
                             rows-set
                             (index-of (schema->lst-names/types #t schema '()) (lt-f-name f))
                             (lt-f-val f)
                             #f
                             #t
                             (set))]))
  ;Main selection
  (let ([schema (table-schema tab)])
    (table schema
           (set->list (eval-form schema
                                 (list->set (table-rows tab))
                                 form)))))

(sort (list '(1 'cos) '(5 'costam) '(-5 'cosinnego)) #:key (lambda (l) (list-ref l 0)) <)
'((-5 'cosinnego) (1 'cos) (5 'costam))
