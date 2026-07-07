# 配置LLVM

LLVM（Low Level Virtual Machine）动态编译技术可以为每个查询生成定制化的机器码用于替换原本的通用函数。通过减少实际查询时冗余的条件逻辑判断、虚函数调用并提高数据局域性，从而达到提升查询整体性能的目的。

由于LLVM需要消耗额外的时间预生成IR中间态表示并编译成机器码，因此在小数据量场景或查询本身耗时较少时，可能引起性能的劣化。

- **[LLVM适用场景与限制](llvm_application_scenarios_and_restrictions.md)**  

- **[其他因素对LLVM性能的影响](other_factors_affecting_llvm_performance.md)**  

- **[LLVM使用建议](recommended_suggestions_for_llvm.md)**  
