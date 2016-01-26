(require 'oauth2)
(require 'json)
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
   (replace-regexp-in-string
    "\n" " "
    (buffer-substring-no-properties start end))))
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
              (concat url "?maxReulst=1000")))))
        (json nil))
    (save-excursion
      (set-buffer buffer)
      (goto-char (point-min))
      (re-search-forward "^$" nil 'move)
      (setq json (cons (json-read-from-string (buffer-substring-no-properties (point) (point-max))) forward))
      (kill-buffer (current-buffer)))
    (let ((all-data (car json)))
      (if (assoc 'nextPageToken all-data)
          (bigquery-get-json url nil nil (cdr (assoc 'nextPageToken all-data)) json  )  
        json))))

(defun bigquery-list-tables (dataset)
  "List all tables in a dataset"
  (interactive "M Dataset:")
  
  (let ((json-object 
         (bigquery-get-json
          (bigquery-url-builder
           (concat "datasets/" dataset "/tables"))))
        (buffer (get-buffer-create "*Big Query*")))
    (set-buffer buffer)
    (delete-region (point-min) (point-max))
    (dotimes (i (length json-object))
      (let ((page (elt json-object i)))
        (let ((data (cdr (assoc 'tables page))))
          (dotimes (i (length data))
            (let ((datum (elt data i)))
              (insert (format "%s"
                              (cdr
                               (assoc 'tableId
                                      (cdr (assoc 'tableReference datum))))))
              (newline)))))))
  (sort-lines nil (point-min) (point-max)))

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

(defmacro for-each (item list &rest body)
  `(dotimes (i (length ,list))
    (let* ((,item (elt ,list i)))
      ,@body)))

(defun bigquery-get-query-results (jobid)
  "Get the first page of results from Big Query"
  (let* ((json-object
          (bigquery-get-json
           (bigquery-url-builder (concat "queries/" jobid))))
         (buffer (get-buffer-create "*BQ Results*"))
         (fields (cdr (assoc 'fields (cdr (assoc 'schema json-object)))))
         (data (cdr (assoc 'rows json-object))))
    (set-buffer buffer)
    (delete-region (point-min) (point-max))
    (for-each field fields 
              (insert (format "%s," (cdr (assoc 'name field)))))
    (newline)
    (for-each row data
              (for-each col (cdar row)
                        (insert (format "%s," (cdar col))))
              (newline))))


(defun bigquery-query (query)
  "Execute a query on bigquery"
  (let* ((json-object
          (bigquery-get-json
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
                                              :query %s)))) bigquery-project-id (bigquery-crap-uuid) query)))
         (jobid (cdr (assoc 'jobId (cdr (assoc 'jobReference json-object))))))
    (bigquery-check-job jobid)
    (bigquery-get-query-results jobid)))


