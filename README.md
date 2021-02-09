# pg_auditor #

PostgreSQL auditing extension that records each data modification statement of specific tables, and allows partial or complete flashback of transactions.

## Description ##

`pg_auditor` provides an interface to put specific tables under audit monitoring, by keeping log of every change of the data being made (by either INSERT, UPDATE or DELETE statements). When all the three data modification statements are recorded, it is possible to make a complete transaction flashback, thus restoring the data to a previous state.

## Prerequisites ##

The `hstore` extension is required in order to store table row versions.

## Functions ##

### Auditing control functions ###

The functions below are used to specify which tables should be put under audit control. Each one of them requires a _SHARE ROW EXCLUSIVE_ lock over the table, so be careful when dealing with write busy tables and use appropriate _lock timeout_ setting if necessary.

* `auditor.attach(regclass [, INSERT [, UPDATE [, DELETE [, TRUNCATE ]]]])` - puts the specified table under audit control. The optional variadic argument(s) could be passed to indicate which statement(s) to be recorded. By default all the data modification statements are logged.
* `auditor.detach(regclass)` - removes the specified table from audit control.
* `auditor.forbid_truncate(regclass)` - protects the specified table from truncate commands, which are impossible to audit or flashback.
* `auditor.allow_truncate(regclass)` - removes truncate protection from the specified table.

### Flashback functions ###

Flashing back of transactions is made by replaying the audit log in a reverse order. Restoring data to a previous state will be applied only for the tables under audit control. If all the tables are put under control. then all the data will be restored.

* `auditor.undo([steps[, override others]])` - undoes the last _steps_ transactions within the current session. If the second argument is set to _true_, then the process will go out of the current session scope, thus overriding concurrent transaction actions.
* `auditor.cancel(transaction bigint)` - undoes the actions made within the specified transaction.
* `auditor.flashback(transaction bigint)` - restores the state of the data as it was after the commit of the specified transaction.
* `auditor.flashback(timestamp)` - restores the state of the data as it was in the specified timestamp.

### Custom data functions ###

* `auditor.get_custom_data()` - the _hstore_ returned by this function is recorded for each audit line, mind overriding it in order to record some application specific data (session variables, etc).

## Tables and set returning functions ##

* `auditor.log` - the audit table, holding the data from all the monitored tables.
* `auditor.evolution(p_relname regclass, p_field_name name, pk_value anyelement)` - returns the complete evolution of a table column, identified by a logical primary key.

## Examples ##

```
postgres=# CREATE EXTENSION pg_auditor CASCADE;
NOTICE:  installing required extension "hstore"
CREATE EXTENSION

postgres=# CREATE TABLE fruit (name text primary key, weight numeric(5, 2));
CREATE TABLE

postgres=# SELECT auditor.attach('fruit');
 attach
--------
 t
(1 row)

postgres=# INSERT INTO fruit (name, weight) VALUES ('orange', 1.34),
                                                   ('banana', 2.60),
                                                   ('mango',  3.14);
INSERT 0 3

postgres=# UPDATE fruit SET weight = weight + 2.9 WHERE name = 'orange';
UPDATE 1

postgres=# DELETE FROM fruit WHERE name = 'mango';
DELETE 1

postgres=# SELECT auditor.undo();
 undo
------
    1
(1 row)

postgres=# TABLE fruit;
  name  | weight
--------+--------
 banana |   2.60
 orange |   4.24
 mango  |   3.14
(3 rows)

postgres=# UPDATE fruit SET weight = weight - 3 WHERE name = 'orange';
UPDATE 1

postgres=# DELETE FROM fruit WHERE name = 'banana';
DELETE 1

postgres=# SELECT transaction_id AS xid, operation, old_rec, new_rec FROM auditor.log;
 xid  | operation |              old_rec               |              new_rec
------+-----------+------------------------------------+------------------------------------
 5556 | INSERT    |                                    | "name"=>"orange", "weight"=>"1.34"
 5556 | INSERT    |                                    | "name"=>"banana", "weight"=>"2.60"
 5556 | INSERT    |                                    | "name"=>"mango", "weight"=>"3.14"
 5557 | UPDATE    | "name"=>"orange", "weight"=>"1.34" | "name"=>"orange", "weight"=>"4.24"
 5558 | DELETE    | "name"=>"mango", "weight"=>"3.14"  |
 5559 | INSERT    |                                    | "name"=>"mango", "weight"=>"3.14"
 5560 | UPDATE    | "name"=>"orange", "weight"=>"4.24" | "name"=>"orange", "weight"=>"1.24"
 5561 | DELETE    | "name"=>"banana", "weight"=>"2.60" |
(8 rows)

postgres=# SELECT * FROM auditor.evolution('fruit'::regclass, 'weight', 'orange'::text);
 transaction_id |       clock_datetime       | operation | old  | new
----------------+----------------------------+-----------+------+------
           5556 | 2021-02-08 14:40:48.562821 | INSERT    |      | 1.34
           5557 | 2021-02-08 14:41:13.087    | UPDATE    | 1.34 | 4.24
           5560 | 2021-02-08 14:41:51.271891 | UPDATE    | 4.24 | 1.24
(3 rows)

postgres=# SELECT auditor.flashback(5556);
 flashback
-----------
         5
(1 row)

postgres=# TABLE fruit;
  name  | weight
--------+--------
 banana |   2.60
 mango  |   3.14
 orange |   1.34
(3 rows)
```

## (Un)known issues ##

- Flashing back of transactions involving foreign key dependencies might fail under some circumstances;
- Flashing back of transactions might malfunction in case the primary key itself of the table has been updated;
- The behaviour of `bytea` columns is unknown.
