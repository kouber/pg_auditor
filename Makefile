EXTENSION = pg_auditor
DATA = $(wildcard src/pg_auditor--*.sql)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
