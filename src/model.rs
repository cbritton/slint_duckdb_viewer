use slint::SharedString;
use slint::StandardListViewItem;
use slint::TableColumn;

#[derive(Debug)]
pub struct QueryResult {
    pub column_names: Vec<TableColumn>,
    pub rows: Vec<Vec<StandardListViewItem>>,
    pub row_count: i32,
    pub duration: std::time::Duration,
}

#[derive(Clone)]
pub struct Filename(pub SharedString);
pub struct PageNumber(pub i32);
pub struct PageSize(pub i32);
pub struct SortIndex(pub i32);
pub struct SortOrder(pub i32);
