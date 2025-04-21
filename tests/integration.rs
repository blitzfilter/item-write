use item_core::item_model::ItemModel;
use item_core::item_state::ItemState;
use item_read::item::get_item_events_by_item_id_sort_latest;
use item_write::write_item;
use test_api::dynamodb::get_client;
use test_api::test_api_macros::blitzfilter_dynamodb_test;

#[blitzfilter_dynamodb_test]
async fn should_create_item_when_write_new_item() {
    let item = ItemModel {
        item_id: "https://foo.bar#123456".to_string(),
        created: Some("2010-01-01T12:00:00.001+01:00".to_string()),
        source_id: Some("https://foo.bar".to_string()),
        event_id: Some("https://foo.bar#123456#2010-01-01T12:00:00.001+01:00".to_string()),
        state: Some(ItemState::AVAILABLE),
        price: Some(42f32),
        category: Some("foo".to_string()),
        name_en: Some("bar".to_string()),
        description_en: Some("baz".to_string()),
        name_de: Some("balken".to_string()),
        description_de: Some("basis".to_string()),
        url: Some("https://foo.bar?item=123456".to_string()),
        image_url: Some("https://foo.bar?item_img=123456".to_string()),
        hash: Some("1d10a63438fff3ccd4877c2195c0a377a6ee0c8caad97e652b1e69c68b45557b".to_string()),
    };

    let client = get_client().await;
    let write_res = write_item(&item, client).await;
    assert!(write_res.is_ok());

    // verify write
    let read_items_res = get_item_events_by_item_id_sort_latest(&item.item_id, client).await;
    assert!(read_items_res.is_ok());
    
    let read_items = read_items_res.unwrap();
    assert_eq!(read_items.len(), 1);
    assert_eq!(*read_items.first().unwrap(), item);
}
