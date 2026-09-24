//! The on-disk format version (spec D3). Written into every WAL segment header and into
//! the data directory's `FORMAT` file. Bump only together with a registered migration.
pub const FORMAT_VERSION: u32 = 2;
