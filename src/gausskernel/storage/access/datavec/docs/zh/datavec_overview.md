# DataVec向量数据库

## 可获得性
本特性自openGauss 6.0.3版本开始引入。

## 特性简介

openGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。

DataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。

## 快速部署指南

DataVec向量数据库可通过[容器镜像安装](https://docs.opengauss.org/zh/docs/latest/installation_guide/installing_the_container_image.html)快速部署，并快速对接大模型，打造本地RAG智能问答服务。

DataVec向量数据库可通过安装[spqplugin_v2插件](../extension_reference/spqplugin_v2.md)，在大数据量场景下获得数据库分布式存储检索能力。

## 客户价值

向量数据库通过高效的相似性搜索能力，让大模型能精准召回相关私有知识，从而在检索增强生成（RAG）、推荐、语义搜索等场景中实现更低延迟、更高准确率的企业级AI应用。

## 特性描述

DataVec能够无缝对接自研大模型。通过嵌入技术将非结构化数据（如文本、图像等）转换为向量数据，DataVec为之提供存储和检索能力。嵌入是一种将非结构化数据映射到向量空间的技术，使得相似文本、图像在向量空间中的距离相近，从而提高检索的准确性和效率。

此外，DataVec还支持鲲鹏指令集加速，实现毫秒级响应。鲲鹏指令集是华为自主研发的一套高性能计算指令集，能够显著提升数据处理和计算的效率。通过利用鲲鹏指令集，DataVec可以在处理大规模向量数据时，提供更快的响应速度和更高的处理能力。

在实际应用中，DataVec可以广泛应用于各种需要高效向量检索的场景。例如，在推荐系统中，DataVec可以根据用户的历史行为和偏好，快速找到与用户兴趣相似的内容，从而提供个性化的推荐。在图像检索中，DataVec可以通过图像特征向量，快速找到与查询图像相似的图片。在自然语言处理(NLP)中，DataVec可以通过文本嵌入，快速找到与查询文本语义相似的文档。

DataVec架构与特性实现详情可参考[向量存储引擎](datavec_architecture.md)介绍。

### 向量数据类型

- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维
- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维
- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数
- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维

>[!NOTE]说明
这里的最高维度是在使用索引场景下的最大维度上限值。

支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)

### 索引支持

- [IVFFLAT](./vector_index.md#ivfflat)  倒排索引
- [IVF-PQ](./pq.md)  PQ量化压缩倒排索引
- [IVF-RabitQ](./RabitQ.md)  RabitQ量化压缩倒排索引
- [HNSW](./vector_index.md#hnsw)  图索引
- [HNSW-PQ](./pq.md)  PQ量化压缩图索引
- [HNSW-RabitQ](./RabitQ.md)  RabitQ量化压缩图索引

## 生态对接

openGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。
更详细的指导，参考[向量数据库工具编排使用](dify.md)

## 使用场景

- 图像识别：用于安全监控、身份验证等场景，通过分析图像中的人脸特征进行识别。
- 车辆检索：通过摄像头捕捉车辆图像，进行车牌识别和车辆特征分析。
- 实时轨迹跟踪：在物流行业，通过实时跟踪获取运输轨迹，提高物流效率和安全性。
- 推荐系统：根据用户浏览和购买力，推荐相关产品，提高用户满意度。
- 声纹匹配：在金融、安防等领域，通过声纹识别技术进行身份验证，确保交易和操作的安全性。
- 基因筛选：在药物研发过程中，通过检索特定基因序列，找到潜在的药物靶点，加速新药研发。

这些应用场景展示了DataVec在各个领域的能力，用户可以自由的将向量数据库使能到各个应用中去。通过[向量数据库教程案例](opengauss_ragpratice.md)， 我们为你展示了向量数据库的多种应用模式。
