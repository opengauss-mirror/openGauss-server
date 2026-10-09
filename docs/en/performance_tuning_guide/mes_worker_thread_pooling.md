# MES Worker Thread Pooling

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:49:57.179Z -->

## Availability<a name="section15406143204715"></a>

This feature is introduced since openGauss 6.0.0-RC1 and applies only to the resource pooling architecture.

## Feature Description<a name="section740615433477"></a>

Under the resource pooling architecture, MES worker threads provide pooling functionality and dynamically adjust the number of worker threads based on inter-node message pressure.

## Customer Benefits<a name="section13406743164715"></a>

In the resource pooling architecture, worker threads are dynamically adjusted to better utilize CPU resources.

## Feature Description<a name="section16406154310471"></a>

Under the resource pooling architecture, MES worker threads provide a pooling option. When enabled, worker threads are managed in a thread pool manner. The system automatically increases the number of worker threads when message pressure is high, and automatically decreases them when message pressure is low. This enables more efficient CPU utilization and improves software availability.

## Feature Enhancements<a name="section1340684315478"></a>

This feature is an extension of the original fixed MES worker thread configuration approach under the resource pooling architecture.

## Feature Constraints<a name="section06531946143616"></a>

- Under the resource pooling architecture, the ss_work_thread_pool_attr parameter indicates whether MES worker thread pooling is enabled. For details about this parameter, see [Resource Pooling Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/resource_pooling_parameters.html).
- Under the resource pooling architecture, MES provides an order-preserving capability. When the order-preserving feature is enabled, messages sent by the same service thread are processed by the same worker thread, ensuring that the order of message sending matches the order of message processing. Currently, this order-preserving feature is unavailable when worker thread pooling is enabled. The DMS component does not use the MES order-preserving capability, and DMS disables this capability by default. Users cannot enable it either. Therefore, enabling worker thread pooling and disabling the order-preserving capability has no impact on current users.

## Dependencies<a name="section8406643144716"></a>

None