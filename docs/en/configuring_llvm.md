# Configuring LLVM<a name="EN-US_TOPIC_0245374538"></a>

Low Level Virtual Machine \(LLVM\) dynamic compilation can be used to generate customized machine code for each query to replace original common functions. Query performance is improved by reducing redundant judgment conditions and virtual function calls, and by making local data more accurate during actual queries.

LLVM needs to consume extra time to pre-generate intermediate representation \(IR\) and compile it into codes. Therefore, if the data volume is small or if a query itself consumes less time, the performance deteriorates.

- **[LLVM Application Scenarios and Restrictions](llvm_application_scenarios_and_restrictions.md)**  

- **[Other Factors Affecting LLVM Performance](other_factors_affecting_llvm_performance.md)**  

- **[Recommended Suggestions for LLVM](recommended_suggestions_for_llvm.md)**  
