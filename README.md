# RocketDB :rocket:

# Motivations

Redis approach to transactions is based on a check-and-set (**CAS**) behavior provided by the **WATCH** command, which accepts a list of keys. The **WATCH** command is usually followed by a **MULTI**-**EXEC** block, which is only executed if any of the watched keys has been modified in the meantime by any other concurrent transaction. For example:
```
WATCH mykey
val = GET mykey
val = val + 1
MULTI
SET mykey $val
EXEC
```

While **WATCH** can be called multiple times before the committing the transaction, serializability can be achieved only if the full set of keys to be watched is known in advance and passed to the first **WATCH** call.

**RocketDB** is meant to unlock the full power of Redis transactions while providing an easy to use interface.

# Installation

To start using RocketDB, type the following command:
```bash
$ go get -u github.com/ostafen/rocketdb
```

# Opening a database

```go
import (
    "github.com/ostafen/rocketdb"
)

const (
    host = "localhost"
    port = 6379
)

func main() {
    db, err := rocketdb.New(redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: "",
		DB:       0,
	}))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ...
}
```

# Transactions

The easiest way to start a new (read-write) transaction is to use the `Update()` method

```go
err := db.Update(func(tx *rocketdb.Tx) error {
    ...
    return nil
})
```

or, for read-only transactions, the `View()` method:

```go
err := db.View(func(tx *rocketdb.Tx) error {
    ...
    return nil
})
```

Sometimes you may want to take care of transaction managment. In those cases, you can use the `Begin()` method to get a new transaction:

```go
tx, err := db.Begin(context.Background(), true) // start a new writable transaction with the provided context
if err != nil {
    return err
}
...
```

# Contact

Stefano Scafiti @ostafen

# Licence
RocketDB source code is available under the **MIT License**.