(require 'oauth2)
(require 'json)
(require 'helm)
(random t)

(defvar bigquery-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") 'bigquery-send-paragraph)
    (define-key map (kbd "C-c l") 'bigquery-list-tables)
    map)
  "Keymap for BQ mode")


(define-derived-mode bigquery-mode sql-mode "Big Query")

;;; Allow customization

(defgroup BIGQUERY nil
  "Querying Big Query from Emacs."
  :version "1"
  :group 'languages)

;; These 3 variables wil be used as defaults, if set.
(defcustom bigquery-client-id ""
  "Client ID from Google Cloud API setup. This value will be
stored in your init file and could be dangerous"
  :type 'string
  :group 'BIGQUERY
  :safe 'stringp)

(defcustom bigquery-client-secret ""
  "Client Secret from Google Cloud API setup. This value will be
  stored in your init file and could be dangerous."
  :type 'string
  :group 'BIGQUERY
  :risky t)

(defcustom bigquery-project-id ""
  "Google Cloud project ID"
  :type 'string
  :group 'BIGQUERY
  :safe 'stringp)

(defvar bigquery-google-auth-url "https://accounts.google.com/o/oauth2/v2/auth")
(defvar bigquery-google-token-url "https://www.googleapis.com/oauth2/v4/token")
(defvar bigquery-scopes "https://www.googleapis.com/auth/bigquery")

(defun bigquery-send-paragraph ()
  "Send the current paragraph to Big Query"
  (interactive)
  (let ((start (save-excursion
                 (backward-paragraph)
                 (point)))
        (end (save-excursion
               (forward-paragraph)
               (point))))
    (bigquery-send-region start end)))

(defun bigquery-send-region (start end)
  "Send a region to Big Query"
  (interactive "r")
  (bigquery-query
   (replace-regexp-in-string "\n" " " (buffer-substring-no-properties start end))))
;(bigquery-query (buffer-substring-no-properties start end))

(defun bigquery-token  ()
  "Call the big query shizzle"
  (oauth2-auth-and-store bigquery-google-auth-url 
                         bigquery-google-token-url 
                         bigquery-scopes
                         bigquery-client-id
                         bigquery-client-secret))

(defun bigquery-url-builder (tail)
  "Build url for request to Google"
  (concat "https://www.googleapis.com/bigquery/v2/projects/" bigquery-project-id "/" tail ))


; TODO: All interactions should be in a buffer called big query
(defun bigquery-switch-to-url-buffer (status)
  "Switch to the buffer returned by `url-retreive'.
    The buffer contains the raw HTTP response sent by the server."
  (switch-to-buffer (current-buffer))
  (search-forward "\n\n")
  (delete-region (point-min) (point))
  (json-read-from-string (buffer-string)))

(defun bigquery-get-json (url &optional method data nextpagetoken forward)
  "Retrieve a json object. If there are multiple pages of data
then get them all and turn into a big list"
  (let ((buffer
         (if (and method data)
             (oauth2-url-retrieve-synchronously (bigquery-token)
                                                url
                                                method
                                                data
                                                '(("Content-Type" . "application/json")))
           (oauth2-url-retrieve-synchronously
            (bigquery-token)
            (if nextpagetoken
                (concat  url "?pageToken=" nextpagetoken)
              (concat url "?maxResults=1000")))))
        (json nil))
    (save-excursion
      (set-buffer buffer)
      (goto-char (point-min))
      (re-search-forward "^$" nil 'move)
      (setq json (cons (json-read-from-string (buffer-substring-no-properties (point) (point-max))) forward))
      (kill-buffer (current-buffer)))
    (let ((all-data (car json)))
      (if (and  (assoc 'nextPageToken all-data)
                (or (not (eq (assoc 'state  (assoc 'status all-data)) "RUNNING"))
                    (not (assoc 'message (assoc 'errorResult  (assoc 'status all-data))) "ERROR")))
                 (bigquery-get-json url nil nil (cdr (assoc 'nextPageToken all-data)) json)
        json))))


(defun bigquery/get-list (url-path set-name set-id set-reference)
  "Print a list of elements one by one to a buffer"
  (let ((json-object 
         (bigquery-get-json
          (bigquery-url-builder (concat  url-path)))))
    (print (format "%s" json-object))
    (mapcan (lambda (x)
              (let ((data (cdr (assoc set-name x))))
                (mapcar (lambda (y)
                          (cdr (assoc set-id (cdr (assoc set-reference y))))) data)))
            json-object)))

(defun bigquery/get-list-to-buffer (url-path buffer-name set-name set-id set-reference)
  "Print a list of elements one by one to a buffer"
  (let ((results (bigquery/get-list url-path set-name set-id set-reference))
        (buffer (get-buffer-create buffer-name)))
    (set-buffer buffer)
    (delete-region (point-min) (point-max))
    (mapc (lambda (x)
            (insert (format "%s" x))
            (newline))
          results)
  (sort-lines nil (point-min) (point-max))))

(defun bigquery/list-datasets ()
  "List all tables in a dataset"
  (interactive)
  (bigquery/get-list-to-buffer "datasets"
                               "*Big Query: Datasets*"
                               'datasets
                               'datasetId
                               'datasetReference))

(defun bigquery/list-tables (dataset)
  "List all tables in a dataset"
  (interactive "M Dataset:")
  (bigquery/get-list-to-buffer (concat "datasets/" dataset "/tables")
                               (concat "*Big Query: " dataset "*")
                               'tables
                               'tableId
                               'tableReference))

(defun bigquery-crap-uuid ()
  "Generate sloppy uuid from xah lee"
   (format "%04x%04x_%04x_%04x_%04x_%06x%06x"
           (random (expt 16 4))
           (random (expt 16 4))
           (random (expt 16 4))
           (random (expt 16 4))
           (random (expt 16 4))
           (random (expt 16 6))
           (random (expt 16 6))))

(defun bigquery-check-job (jobid)
  (let* ((json-object
          (bigquery-get-json
           (bigquery-url-builder (concat "jobs/" jobid))))
         (status  (cdr (assoc 'state (cdr (assoc 'status json-object))))))
    (if (eq status "RUNNING")
        (progn 
          (sleep-for 10)
          (bigquery-check-job jobid)))))

(defun bigquery-get-query-results (jobid)
  "Get the first page of results from Big Query"
  (let* ((json-object
          (bigquery-get-json
           (bigquery-url-builder (concat "queries/" jobid))))
         (buffer (get-buffer-create "*BQ Results*")))
    (set-buffer buffer)
    (delete-region (point-min) (point-max))
    (mapc (lambda (x)
            (let ((fields (cdr (assoc 'fields (cdr (assoc 'schema x)))))
                  (data (cdr (assoc 'rows x))))
              (mapc (lambda (field) (insert (format "%s," (cdr (assoc 'name field))))) fields)
              (newline)
              (mapc (lambda (row)
                      (mapc (lambda (col)
                              (insert (format "%s," (cdar col))))
                            (cdar row)) 
                      (newline))
                    data))) json-object)))

(defun bigquery-query (query)
  "Execute a query on bigquery"
  (let* ((json-object
          (car (bigquery-get-json
                (bigquery-url-builder (concat "jobs"))
                "POST"
                (format  (json-encode
                          '(:kind "bigquery#job"

                                  :configuration (:query
                                                  (:destinationTable
                                                   (:projectId %s
                                                               :datasetId "tmp"
                                                               :tableId %s)
                                                   :defaultDataset "tmp"
                                                   :query %s)))) bigquery-project-id (bigquery-crap-uuid) query))))
         (jobid (cdr (assoc 'jobId (cdr (assoc 'jobReference json-object))))))
    (if jobid
        (progn (bigquery-check-job jobid)
               (bigquery-get-query-results jobid)))))

(defun bigquery/list-tables-helm (candidate)
  (bigquery/get-list (concat "datasets/" candidate "/tables")
                     'tables
                     'tableId
                     'tableReference))

(defun bigquery/list-datasets-helm ()
  (bigquery/get-list "datasets"
                     'datasets
                     'datasetId
                     'datasetReference))

(defun helm-bigquery-insert-tables (candidate)
  (insert (format "%s" candidate)))

(defvar helm-bigquery-dataset-actions
  (helm-make-actions
   "Open dataset"              #'helm-bigquery-view-tables))

(defvar helm-source-bigquery-tables
    '((name . "Big Query Tables")
      (action . ("Insert table name" . helm-bigquery-insert-tables))
      (candidates-process . bigquery/list-tables-helm)
      (action-transformer . nil)))

(defun helm-bigquery-view-tables (candidate)
  "Insert big query table name at current location"
  (message candidate)
  (helm :sources     '((name . "Big Query Tables")
                       (action . (("Insert table name" . helm-bigquery-insert-tables)))
                       (candidates . (lambda () (mapcar (lambda (x) (concat candidate "." x)) (bigquery/list-tables-helm candidate))))
      (action-transformer . nil))
	:buffer "*helm-bigquery*"))


(defvar helm-source-bigquery-datasets
    '((name . "Big Query")
      (action . helm-bigquery-dataset-actions)
    (candidates-process . bigquery/list-datasets-helm)
    (action-transformer . nil)))

(defun helm-bigquery ()
  "Bring up a Spotify search interface in helm."
  (interactive)
  (helm :sources '(helm-source-bigquery-datasets)
	:buffer "*helm-bigquery*"))
