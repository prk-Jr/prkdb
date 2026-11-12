# SQLx SQLite dependency patch

This directory contains `sqlx-sqlite` 0.8.6 from crates.io, under its original
MIT / Apache-2.0 licenses. Its Rust source is unchanged. Upstream revision:
`bab1b022bd56a64f9a08b46b36b97c5cff19d77e` (`sqlx-sqlite/`).

The sole upstream manifest change upgrades `libsqlite3-sys` from `0.30.1` to
`0.39.1`, which bundles SQLite 3.54.1. SQLite versions before 3.54.0 can disclose
uninitialized memory when opening a crafted database and mismatched WAL file
([RUSTSEC-2026-0459](https://rustsec.org/advisories/RUSTSEC-2026-0459)).

SQLx 0.8.6 requires `libsqlite3-sys` 0.30.x, and SQLx 0.9.0 still requires a
version below 0.38. Neither accepts the patched 0.39 releases. The workspace
patch retains SQLx 0.8's API while using the fixed bundled SQLite, without an
advisory exemption or a system-library requirement.

Remove the workspace `[patch.crates-io]` entry and this directory when an
upstream SQLx release accepts patched SQLite and its migration is validated.
Use `cargo deny check` and the SQL adapter, ORM, workspace and crash-harness
checks when updating this patch. Keep upstream license files with the source.

## Consumption scope

Cargo applies `[patch]` only from the root workspace. Builds and tests of this
workspace use the fixed SQLite library. Applications depending on published or
git versions of prkdb do not inherit this override; their root manifest must
apply the same patch until upstream supports patched SQLite. Publishing and
downstream dependency resolution require separate validation; this patch does
not establish that downstream consumers have upgraded.
