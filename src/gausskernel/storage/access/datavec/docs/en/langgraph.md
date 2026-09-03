# Deploying LangGraph with openGauss

LangGraph is a low-level orchestration framework for building, managing, and deploying long-running, stateful agents.

## Containerized Deployment of openGauss

For details, see [Container Image Installation](../installation_guide/installing_the_container_image.md).

## Installation

Code archive address:
[https://gitee.com/kunpeng_compute/KunpengRAG.git](https://gitee.com/kunpeng_compute/KunpengRAG.git)

Use the following command to install the source code:
```bash
git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
cd KunpengRAG/langgraph/checkpoint-opengauss
pip install .
```

## Usage

The following is the simplest example of using OpenGaussStore:

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
