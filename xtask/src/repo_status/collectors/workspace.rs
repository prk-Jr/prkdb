pub(super) fn detect_codegen_support(codegen_command: Option<&str>) -> bool {
    codegen_command.is_some_and(|text| {
        text.contains("Python")
            && text.contains("Typescript")
            && text.contains("Go")
            && text.contains("prkdb-cli serve")
    })
}
