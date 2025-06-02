
pub fn create_leaf_node() -> &[u8] {
}


pub fn find_node(key : &[u8], pager : &mut Pager) {
    // start at root node
    let page_buffer = Pager::allocate_page_buffer();
    let root_id = pager.get_page(&page_buffer, 0);

    // deserialize page contents
}
