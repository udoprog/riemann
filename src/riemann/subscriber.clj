(ns riemann.subscriber
  (:require [riemann.instrumentation :refer [Instrumented]]
            [riemann.service :refer [Service ServiceEquiv]]
            [riemann.streams :refer [call-rescue]]
            [riemann.pubsub :as p]
            [clojure.tools.logging :refer [info warn]]))

(defrecord SubscriberService [channel stream core open-sub]
  ServiceEquiv
  (equiv? [this other]
          (and (instance? SubscriberService other)
               (= channel (:channel other))
               (= stream (:stream other))))

  Service
  (conflict? [this other]
             (and (instance? SubscriberService other)
                  (= channel (:channel other))
                  (= stream (:stream other))))

  (reload! [this new-core]
    (locking this
      (let [old-sub @open-sub old-core @core]
        (when-not (or (nil? old-core) (nil? old-sub))
          (p/unsubscribe! old-core old-sub))

        (let [new-sub
              (p/subscribe!
                (:pubsub new-core) channel
                stream
                true)]
          (reset! open-sub new-sub))))

    (reset! core new-core))

  (start! [this])
  (stop! [this]))

(defn subscriber
  "Allow the user to subscribe to any internal topics."
  [channel & children]
  (SubscriberService.
    channel
    (fn [e] (call-rescue e children))
    (atom nil)
    (atom nil)))
