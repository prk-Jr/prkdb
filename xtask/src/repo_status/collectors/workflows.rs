pub(super) fn benchmark_job_present(ci_workflow: Option<&str>) -> bool {
    ci_workflow.is_some_and(|text| text.contains("benchmark"))
}
