# tinystore

- Inspired by [yakv](https://github.com/knizhnik/yakv)

> [!NOTE]
> In early stages of development, the goal is a:

Simple, distributed key value storage engine.
Current target architecture consists of a page cache, b+tree and application connection object.  

```mermaid
flowchart TD
    User -- API --> Connection
    Connection -- DB Access --> bt[BTree]
    bt -- Buffer access --> PageCache
    PageCache -- Disk I/O --> Connection
```
