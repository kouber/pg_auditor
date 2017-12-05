pg_auditor
==========

PostgreSQL auditing extension that records each data modification statement to selected tables and allows partial or complete flashback of transactions.

# Installation

Once you clone the project, simply run (in top directory):

`$ make install`

Then, in PostgreSQL you can do:

`$ CREATE EXTENSION pg_auditor;`

That will effectively create all the necessary functions to start or stop table auditing. By default they are loaded into the `auditor` schema.

## Callback function



# Public API

## Auditing control functions

auditor.attach(regclass)
auditor.detach(regclass)

auditor.forbid_truncate(regclass)
auditor.allow_truncate(regclass)

## Flashback functions

auditor.undo([steps[, override others]])
auditor.cancel(transaction bigint)

auditor.flashback(transaction bigint)
auditor.flashback(timestamp)

## Tables and set returning functions

auditor.log
auditor.evolution(p_relname regclass, p_field_name name, pk_value anyelement)



AccessExclusiveLock

# Examples



# (Un)known issues

- Flashing back of transactions involving foreign key dependencies might fail under some circumstances
- Flashing back of transactions might malfunction in case the primary key itself of a table has been updated
- The behaviour of `bytea` columns is unknown