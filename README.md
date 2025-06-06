# tinystore

> [!NOTE]
> In early stages of development, the goal is a:

Simple, distributed key value storage engine.
Current architecture consists of a page cache, b+tree and application connection object.  

```mermaid
flowchart TD
    User -- API --> Connection
    Connection --> ops[operations]
    ops -- Serialization and Organization --> B+Tree
    ops -- Disk I/O --> page_cache[Page Cache]
```
