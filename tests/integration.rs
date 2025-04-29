use item_core::item_model::ItemModel;
use item_read::item::{
    get_item_events_by_item_id_sort_latest, get_item_events_by_item_id_sort_oldest,
};
use item_write::{write_item, write_items};
use test_api::generator::Generator;
use test_api::localstack::get_dynamodb_client;
use test_api::test_api_macros::blitzfilter_dynamodb_test;
use tokio::try_join;

#[blitzfilter_dynamodb_test]
async fn should_create_item_when_write_new_item() {
    let item = ItemModel::generate();

    let client = get_dynamodb_client().await;
    let write_res = write_item(&item, client).await;
    assert!(write_res.is_ok());

    // verify write
    let read_items_res = get_item_events_by_item_id_sort_latest(&item.item_id, client).await;
    assert!(read_items_res.is_ok());

    let read_items = read_items_res.unwrap();
    assert_eq!(read_items.len(), 1);
    assert_eq!(*read_items.first().unwrap(), item);
}

#[blitzfilter_dynamodb_test]
async fn should_create_items_when_write_new_items() {
    let items = ItemModel::generate_n::<2>();

    let client = get_dynamodb_client().await;
    let write_ress = write_items(&items, client).await;
    let write_res = write_ress.get(0).unwrap();
    assert!(write_res.is_ok());

    // verify write
    let read_items_res = try_join!(
        get_item_events_by_item_id_sort_oldest(&items[0].item_id, client),
        get_item_events_by_item_id_sort_oldest(&items[1].item_id, client),
    );
    assert!(read_items_res.is_ok());

    let (read_items1, read_items2) = read_items_res.unwrap();
    assert_eq!(*read_items1.get(0).unwrap(), items[0]);
    assert_eq!(*read_items2.get(0).unwrap(), items[1]);
}

#[blitzfilter_dynamodb_test]
async fn should_write_new_items_when_above_max_batch_size() {
    let items = ItemModel::generate_n::<42>();

    let client = get_dynamodb_client().await;
    let write_ress = write_items(&items, client).await;
    write_ress
        .iter()
        .for_each(|write_res| assert!(write_res.is_ok()))
}
