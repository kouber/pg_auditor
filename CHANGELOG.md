0.2
---
- Fix crash of the `cancel(txid)` function.
- Rename `rec` to `new_rec` in the `log` table.
- Add optional variadic list of DML statements to pass to the `attach(regclass[, DML[, DML]])` function, effectively allowing to audit only specific DML operations, rather than all the operations.

0.1
---
- Initial version.
