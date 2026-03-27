use clap::ValueEnum;

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum Language {
    Python,
    Typescript,
    Go,
    All,
}

/// Use `http://127.0.0.1:8080` for `prkdb-server`, or
/// `http://127.0.0.1:50051` for the local gRPC endpoint exposed by
/// `prkdb-cli serve`.
