# FX framework

Set of Duct modules for rapid clojure development

## Modules

### `:fx.module/autowire`

Module for scanning project namespaces, converting your components to integrant keys and generating Duct config on the fly.
You can use clojure metadata to mark functions or variables as system components:

```clojure
(def ^:fx.module/autowire db-connection
  (jdbc/get-connection {:connection-uri "db-uri"
                        :dbtype         "sqlite"}))
```

```clojure
(defn ^:fx.module/autowire health-check [ctx req]
  {:status :ok})
```

Also, you can specify dependencies for your keys as arguments metadata:

```clojure
(defn ^:fx.module/autowire status
  [^:my-namespace/db-connection db-connection]
  {:status     :ok
   :connection (db-connection)})
```
Notice that components has a namespaced keys.

Also, you can specify `:fx.module/wrap-fn true` to wrap a component for the later usage e.g.  
without wrapping you have to return an anonymous function:

```clojure
(defn select-all-todo-handler
  {:fx.module/autowire true}
  [^:my-namespace/db-connection db _request-params]
  (fn [_]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (jdbc.sql/query db select-all-todo)}))
```

with wrapping:

```clojure
(defn select-all-todo-handler
  {:fx.module/autowire true
   :fx.module/wrap-fn  true}
  [^:my-namespace/db-connection db _request-params]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (jdbc.sql/query db select-all-todo)})
```

For full example check the `fx.demo.todo` namespace.

### Setup

When you first clone this repository, run:

```sh
lein duct setup
```

This will create files for local configuration, and prep your system for the project.

## Developing

To begin developing, start with a REPL.

```sh
lein repl
```

## Run demo project

```sh
lein repl
```

```clojure
user=> (dev)
:loaded
```

Run `go` to prep and initiate the system.

```clojure
dev=> (go)
:initiated
```

### Testing

Testing is fastest through the REPL, as you avoid environment startup time.

```clojure
dev=> (test)
...
```

But you can also run tests through Leiningen.

```sh
lein test
```
