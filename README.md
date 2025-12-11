# SQLancer++

A tool for testing SQL database management systems with automatic dialect adaptability.

## Getting Started

Requirements:
* Java 11 or above
* [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)
* The DBMS that you want to test (embedded DBMSs such as DuckDB, H2, and SQLite do not require a setup)


```
# unzip the file first then get into the directory
cd sqlancer
mvn package -DskipTests
java -jar target/sqlancer-2.0.0.jar --use-reducer general  --database-engine sqlite
```

If the execution prints progress information every five seconds, then the tool works as expected. Note that SQLancer++ might find bugs in SQLite. Before reporting these, be sure to check that they can still be reproduced when using the latest development version. The shortcut CTRL+C can be used to terminate SQLancer++ manually. If SQLancer++ does not find any bugs, it executes infinitely. The option `--num-tries` can be used to control after how many bugs SQLancer++ terminates. Alternatively, the option `--timeout-seconds` can be used to specify the maximum duration that SQLancer++ is allowed to run.


----

## Adapt SQLancer++ to custom DBMSs

Ideally, you should be able to adapt SQLancer++ to your custom DBMS by implementing the methods in `general/GeneralOptions.java`. For example, to test PostgreSQL, at least need to override the `getJDBCString` method.

```Java
//...
POSTGRESQL {
    @Override
    public String getJDBCString(GeneralGlobalState globalState) {
        return String.format("jdbc:postgresql://localhost:10010/?user=postgres&password=postgres");
    }
},
//...
```

By default, SQLancer++ will use `cleanOrSetUpDatabase` method to create a clean space for tables after connecting to the system by using above JDBC String. It will first try to `CREATE DATABASE` and `USE` it (like in MySQL). If it fails, it will try to blindly use `DROP TABLE` to clean all the possible tables.

The general workflow of SQLancer++ is as follows:
1. Connect to a database
2. Setup a new database state
3. Query the database and validate the results

## Steps

1. Add a new Enum object `$DBMS` in the `GeneralDatabaseEngineFactory` class in `GeneralOptions.java`  and implement `getJDBCString` method. Create the JAR by `mvn package -DskipTests`
2. Start your DBMS instance.
3. Begin testing by executing below command:
    `java -jar target/sqlancer-2.0.0.jar --use-reducer general  --database-engine $DBMS`
4. Check `logs/general` or the direct shell output to see if there are any bugs.
    - `*-cur.log`: logs of all the statements executed
    - `*.log`: logs of the statements triggered a potential bug (after reducing if `--use-reducer` is enabled)

## Arguments

You could adjust some arguments to control the testing process. Here are some of them which might be helpful:

- `--num-threads $i`: the number of threads to run the test. The default value is 4. You could set it to a higher value if you have a powerful machine and there are not so many bugs. Set to 1 if there is too many issues.
- `--use-reducer`: enable the reducer to reduce the bug-triggering query. Do not enable it if you want to see the full SQL statements.
- `--oracle $ORACLE`: the oracle to use. The default value is `WHERE`. You could also try `NoREC`.
- `--use-deduplicator`: enable the bug deduplicator to reduce duplication in best effort. To enable it, add `--use-deduplicator` after `general` in the command.
