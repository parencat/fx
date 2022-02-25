# FX framework

Set of Duct modules for rapid clojure development

## Modules

### `:fx.module/autowire`

Module for scanning project namespaces for integrant keys and generating Duct config. You can use clojure metadata to
create an integrant key:

```clojure
(defn ^:fx.module/autowire health-check [ctx req]
  {:status :ok})
```

Also, you can specify dependencies for your keys as arguments metadata:

```clojure
(defn status
  {:fx.module/autowire :http-server/handler}
  [^:fx.demo.something/db-connection db-connection]
  {:status     :ok
   :connection (db-connection)})
```

Also, you can specify `:fx.module/wrap-fn true` to wrap a component for the later usage e.g.  
without wrapping you have to return an anonymous function:

```clojure
(defn select-all-todo-handler
  {:fx.module/autowire true}
  [^:fx.demo.todo/db-connection db _request-params]
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
  [^:fx.demo.todo/db-connection db _request-params]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (jdbc.sql/query db select-all-todo)})
```

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
