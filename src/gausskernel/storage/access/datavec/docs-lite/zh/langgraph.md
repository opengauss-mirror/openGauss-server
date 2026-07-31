# 使用 openGauss 部署 LangGraph

LangGraph 是一个底层编排框架，用于构建、管理和部署长时间运行的有状态代理。

## openGauss 容器化部署

详见[容器镜像安装](https://docs.opengauss.org/zh/docs/latest-lite/installation_guide/installing_the_container_image.html)。

## 安装

代码归档地址：
[https://gitee.com/kunpeng_compute/KunpengRAG.git](https://gitee.com/kunpeng_compute/KunpengRAG.git)

使用以下命令安装源码：
```bash
git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
cd KunpengRAG/langgraph/checkpoint-opengauss
pip install .
```

## 使用

以下是使用 OpenGaussStore 的最简用例：

```python
from langgraph.store.opengauss import OpenGaussStore

DEFAULT_OPENGAUSS_URI = "postgres://postgres:postgres@localhost:5441/postgres"


def main() -> None:
    with OpenGaussStore.from_conn_string(DEFAULT_OPENGAUSS_URI) as store:
        store.setup()
        store.put(("demo",), "item1", {"text": "hello", "count": 1})
        store.put(("demo",), "item2", {"text": "world", "count": 2})

        item = store.get(("demo",), "item1")
        print("get:", item.value if item else None)

        namespaces = store.list_namespaces(prefix=("demo",))
        print("namespaces:", namespaces)

        store.delete(("demo",), "item1")
        print("deleted item1")


if __name__ == "__main__":
    main()

```