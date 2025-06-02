// n children
// n-1 keys


enum NodeType {
    Root(SubTreeRefs),
    Internal(SubTreeRefs),
    Leaf
}

struct SubTreeRefs {
    child_ptrs : Vec<usize>,
    keys : Vec<&[u8]>
}

struct Node {
    node_type : NodeType,
}

impl Node {
    fn serialize() {
    }
}
