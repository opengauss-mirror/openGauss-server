# Hint Specifying Not to Use Global Plan Cache<a name="EN-US_TOPIC_0000001096240528"></a>

## Function<a name="section290819468377"></a>

When global plan cache is enabled, you can use the  **no\_gpc**  hint to force a single query statement not to share the plan cache globally. Only the plan cache within the current session lifecycle is retained.

## Syntax<a name="section530131664410"></a>

```
no_gpc
```

>[!NOTE]NOTE 
>
>This parameter takes effect only for statements executed by PBE when  **enable\_global\_plancache**  is set to  **on**.

## Example<a name="section5736356154"></a>

![](figures/zh-cn_image_0000001144139135.png)

No result exists in the  **dbe\_perf.global\_plancache\_status**  view, that is, no plan is cached globally.
