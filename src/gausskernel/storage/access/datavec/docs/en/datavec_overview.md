# DataVec Vector Database

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T12:07:58.844Z pushedAt=2026-07-30T12:23:58.606Z -->

## Availability

This feature has been available since openGauss 6.0.3.

## Feature Overview

DataVec is a vector engine built on openGauss, providing storage and retrieval for vector data types. It delivers fast and accurate retrieval results when processing large-scale high-dimensional vector data. It is suitable for intelligent apps in various complex scenarios, such as intelligent knowledge retrieval and Retrieval-Augmented Generation (RAG).

DataVec currently supports the following features: exact and approximate nearest neighbor search, L2 distance, cosine distance, inner product, vector indexes, and vector manipulation functions and operators. As a kernel feature of openGauss, DataVec uses familiar SQL syntax to operate on vectors, simplifying the process of using a vector database for users.

## Quick Deployment

DataVec can be rapidly deployed via [container image installation](../installation_guide/installing_the_container_image.md) and quickly integrated with large language models to build local RAG services for intelligent question-answering.

DataVec can leverage the [spqplugin_v2 plugin](../extension_reference/spqplugin_v2.md) to gain distributed storage and retrieval capabilities in large-scale data scenarios.

## Customer Value

With efficient similarity search capabilities, DataVec enables large models to accurately recall relevant private knowledge, thereby achieving lower latency and higher accuracy in enterprise-level AI applications such as RAG, recommendation systems, and semantic search.

## Feature Description

DataVec seamlessly integrates with self-developed large models. By converting unstructured data (such as text and images) into vector data through embedding techniques, DataVec provides storage and retrieval capabilities for such data. Embedding is a technique that maps unstructured data into a vector space, making similar text and images closer in vector space, thereby improving retrieval accuracy and efficiency.

In addition, DataVec supports Kunpeng instruction set acceleration, achieving millisecond-level response. The Kunpeng instruction set is a high-performance computing instruction set independently developed by Huawei, capable of significantly improving the efficiency of data processing and computation. By leveraging the Kunpeng instruction set, DataVec can deliver faster response speeds and higher processing capacity when handling large-scale vector data.

In practical applications, DataVec can be widely applied to various scenarios that require efficient vector retrieval. For example, in recommendation systems, DataVec can quickly find content similar to the user's interests based on the user's historical behavior and preferences, thereby providing personalized recommendations. In image retrieval, DataVec can rapidly locate images similar to a query image through image feature vectors. For natural language processing (NLP), DataVec can quickly find documents semantically similar to a query text through text embeddings.

For details on the DataVec architecture and feature implementation, refer to [Vector Storage Engine](datavec_architecture.md).

### Vector Data Types

- [vector](./vector_data_type.md#vector): float vector, supports up to 16,000 dimensions
- [bitvec](./vector_data_type.md#bit): bit vector, supports up to 83,886,080 dimensions
- [sparsevec](./vector_data_type.md#sparsevec): sparse vector, supports up to 1,000,000,000 dimensions and up to 16,000 non-zero elements
- [halfvec](./vector_data_type.md#halfvec): halfvec vector, supports up to 16,000 dimensions

> [!NOTE]
> The maximum dimensions listed above are the upper limits for index-usage scenarios.

For conversions between vector types and ordinary types, distance calculations, and vector computations, see [Vector Functions and Operators](./vector_functions_and_operators.md).

### Index Support

- [IVFFLAT](./vector_index.md#ivfflat): inverted index
- [IVF-PQ](./pq.md): PQ compressed inverted index
- [IVF-RabitQ](./RabitQ.md): RabitQ compressed inverted index
- [HNSW](./vector_index.md#hnsw): graph index
- [HNSW-PQ](./pq.md): PQ compressed graph index
- [HNSW-RabitQ](./RabitQ.md): RabitQ-compressed graph index

## Ecosystem Integration

openGauss DataVec provides multi-language ecosystem integration with Python, Java, Node.js, Go, and others, enabling vector database capabilities to be quickly activated through API calls. Meanwhile, DataVec embraces open-source third-party components, achieving rapid compatibility and diverse options in RAG scenarios. 
For more detailed guidance, refer to [Vector Database Tool Orchestration and Usage](dify.md).

## Use Cases

- Image recognition: Used in security monitoring, identity verification, and other scenarios, recognizing objects by analyzing their features in images.
- Vehicle retrieval: Capturing vehicle images via cameras for license plate recognition and vehicle feature analysis.
- Real-time trajectory tracking: In the logistics industry, obtaining transport trajectories through real-time tracking to improve logistics efficiency and safety.
- Recommendation systems: Recommending relevant products based on user browsing and purchasing power to improve user satisfaction.
- Voiceprint matching: In fields such as finance and security, voiceprint recognition technology is used for identity verification to ensure the security of transactions and operations.
- Gene screening: In drug R&D, retrieving specific gene sequences to identify potential drug targets, accelerating new drug development.

These use cases demonstrate the capabilities of DataVec across various domains. Users can freely enable it in their apps. Through [Vector Database Tutorials](opengauss_ragpratice.md), we present multiple application patterns.
