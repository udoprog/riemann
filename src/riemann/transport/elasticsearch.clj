(ns riemann.transport.elasticsearch
  (:require [riemann.instrumentation :refer [Instrumented]]
            [riemann.service :refer [Service ServiceEquiv]]
            [riemann.common :refer [event-to-json unix-to-iso8601]]
            [riemann.pubsub :as p]
            [clojure.tools.logging :refer [info warn]])
  (:require [clojurewerkz.elastisch.rest  :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]))

(defn event-to-object
  [e]
  (assoc e "@timestamp" (unix-to-iso8601 (:time e))))

(defrecord ElasticSearchClient [url app-name core open-sub]
  ServiceEquiv
  (equiv? [this other]
          (and (instance? ElasticSearchClient other)
               (= url (:url other))))

  Service
  (conflict? [this other]
             (and (instance? ElasticSearchClient other)
                  (= url (:url other))))

  (reload! [this new-core]
    (locking this
      (let [old-sub @open-sub old-core @core]
        (when-not (or (nil? old-core) (nil? old-sub))
          (p/unsubscribe! old-core old-sub))

        (let [new-sub
              (p/subscribe!
                (:pubsub new-core) "index"
                (fn [e]
                  (let [document (event-to-object e)]
                    (esd/create app-name "event" document)
                    (info (str "Created: " document))))
                true)  ]
          (reset! open-sub new-sub))))

    (reset! core new-core))

  (start! [this]
    (esr/connect! url)
    (locking this
      (info "Forwarding events to ElasticSearch at " url)))

  (stop! [this]
    (locking this
      (info "Forwarding events to ElasticSearch at " url " shut down"))))

(defn elasticsearch-writer
  "Submit indexed events to elasticsearch."
  ([] (elasticsearch-writer {}))
  ([opts]
   (ElasticSearchClient.
     (get opts :url "http://127.0.0.1:9200")
     (get opts :app-name "riemann")
     (atom nil)
     (atom nil))))
