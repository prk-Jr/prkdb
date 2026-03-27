use super::RepoSnapshot;
use crate::repo_status::model::{Confidence, DimensionId, Evidence, Finding, Severity};

pub(super) fn collect(snapshot: &RepoSnapshot) -> Vec<Finding> {
    let mut findings = Vec::new();

    if has_codegen_contract_mismatch(snapshot) {
        findings.push(Finding {
            id: "docs_contract_mismatch".to_owned(),
            dimension: DimensionId::ContractConsistency,
            severity: Severity::Error,
            confidence: Confidence::High,
            message: "Codegen docs point `prkdb-cli serve` users at the HTTP port instead of the local gRPC port.".to_owned(),
            evidence: vec![
                Evidence::new(
                    "docs/guide/codegen.md",
                    "Shows `prkdb codegen --server http://127.0.0.1:8080` for `prkdb-cli serve`.",
                ),
                Evidence::new(
                    "crates/prkdb-cli/src/commands/codegen.rs",
                    "Documents `http://127.0.0.1:50051` as the local gRPC endpoint exposed by `prkdb-cli serve`.",
                ),
            ],
        });
    }

    findings
}

fn has_codegen_contract_mismatch(snapshot: &RepoSnapshot) -> bool {
    let Some(codegen_doc) = snapshot.codegen_doc.as_deref() else {
        return false;
    };
    let Some(codegen_command) = snapshot.codegen_command.as_deref() else {
        return false;
    };

    let docs_use_http_port = codegen_doc.contains("prkdb codegen --server http://127.0.0.1:8080");
    let docs_reference_serve = codegen_doc.contains("prkdb-cli serve");
    let command_documents_grpc_port = codegen_command.contains("http://127.0.0.1:50051")
        && codegen_command.contains("prkdb-cli serve");

    docs_use_http_port && docs_reference_serve && command_documents_grpc_port
}
