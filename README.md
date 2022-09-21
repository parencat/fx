# FX

Set of Duct modules for a rapid clojure development.

## Installation

Assuming you already have a Duct project bootstrapped. 
Add to your leiningen dependencies:

```clojure
[io.github.parencat/fx "0.1.0"]
```

## Modules

### `:fx.module/autowire`

Autowire module purpose is to reduce a hassle while configuring your application components. This module will scan your
project namespaces. Whenever Autowire will find some ns members (vars or functions)
with `:fx/autowire` key in the metadata it will automatically create an integrant key and a configuration map.

To enable autowire module add this key to your Duct `config.edn` file

```clojure
{:duct.profile/base
 {:duct.core/project-ns your-project-root-namespace}

 ;; other profiles and modules

 :fx.module/autowire {}}
```

You can limit a number of scanned namespaces by providing a `:root` path for some file or folder

```clojure
{:duct.profile/base
 {:duct.core/project-ns your-project-root-namespace}

 ;; other profiles and modules

 :fx.module/autowire {:root your-project-root-namespace/components}}
```

After that you can attach `:fx/autowire` key to any members, e.g.

```clojure
(ns my-project.core)

(def ^:fx/autowire db-connection
  (jdbc/get-connection {:connection-uri "db-uri"
                        :dbtype         "sqlite"}))
```

In this case Autowire will create `:my-project.core/db-connection` component and you can reference and use
it in other components. Notice that autowired components always has a namespaced keyword as its name 
(to prevent the components name clashes)

e.g.

```clojure
(let [db (-> (ig/find-derived-1 system :my-project.core/db-connection)
             (val))]
  (jdbc/execute! db ["SELECT * FROM users ..."]))
```

Also, you can specify dependencies for your components as arguments metadata:

```clojure
(defn ^:fx/autowire get-users [^:my-project.core/db-connection db]
  (fn []
    {:status :ok
     :users  (jdbc/execute! db ["SELECT * FROM users ..."])}))
```

This kind of dependency injection already built-in into the Duct framework, 
Autowire is just reducing some boilerplate around it.

#### Additional options

`:fx/halt`

By default, Autowire will create only `ig/init-key` methods for found components. 
It is possible to create a `ig/halt-key!` method as well. 
All you need it's to add `:fx/halt` key with the name of component to the metadata 

```clojure
(ns my-project.core
  (:import
   [java.sql Connection]))

(def ^:fx/autowire db-connection
  (jdbc/get-connection {:connection-uri "db-uri"
                        :dbtype         "sqlite"}))

(defn close-connection
  {:fx/autowire true
   :fx/halt     ::db-connection}
  [^Connection conn]
  (.close conn))
```

`:fx/wrap`

You can add `:fx/wrap` key to wrap a component in the anonymous function for the later usage.  
Without wrapping you'll have to return an anonymous function which holds the dependencies as closures:

```clojure
(defn ^:fx/autowire get-users 
  [^:my-project.core/db-connection db]
  (fn [request]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (jdbc/execute! db ["SELECT * FROM users ..."])}))
```

with wrapping your component will be initialized with the partially applied function:

```clojure
(defn ^:fx/autowire ^:fx/wrap get-users
  [^:my-project.core/db-connection db request]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (jdbc/execute! db ["SELECT * FROM users ..."])})
```

For more examples check  `fx.module.stub-functions` and `fx.module.autowire-test` namespaces.
