// A chaos-gated test. check_chaos_tests_run.sh must reject it when no workflow step
// passes --features chaos for this file.
#[cfg(feature = "chaos")]
#[tokio::test]
async fn a_gated_test_no_workflow_runs() {}
