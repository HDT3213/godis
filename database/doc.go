package database

/*
[Server](https://github.com/HDT3213/godis/blob/master/database/database.go) is an implemention of interface [DB](https://github.com/HDT3213/godis/blob/master/interface/database/db.go).

[server.Handler](https://github.com/HDT3213/godis/blob/master/redis/server/server.go) holds an instance of Server as storage engine, and pass command line to Server through db.Exec method.

Server is a multi-database engine which supports `SELECT` command. Besides multiple database instance, it holds pubsub.Hub and aof.Handler for publish-subscription and AOF persistence.

Server.Exec is the main entry for Server, it handles authentication, publish-subscription, aof as well as system commands itself, and invoke Exec function of selected db for other commands.

[godis.DB.Exec](https://github.com/HDT3213/godis/blob/master/database/single_db.go) handles transaction control command (such as watch, multi, exec) itself, and invokes DB.execNormalCommand to handle normal commands. The word, normal command, is commands which read or write limited keys, can execute within transaction, and supports rollback. For example, get, set, lpush are normal commands, while flushdb, keys are not.

[registerCommand](https://github.com/HDT3213/godis/blob/master/database/router.go) is used for registering normal command. A normal command requires three functions：

- ExecFunc: The function that actually executes the command, such as [execHSet](https://github.com/HDT3213/godis/blob/master/database/hash.go)
- PrepareFunc executes before ExecFunc, it analysises command line and returns read/written keys for lock
- UndoFunc invoked in transaction only, it generates undo log in case need rollback in transaction
*/
