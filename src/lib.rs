use aws_sdk_dynamodb as dynamo_db;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_write_item::{BatchWriteItemError, BatchWriteItemOutput};
use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};
use item_core::item_model::ItemModel;
use serde_dynamo::aws_sdk_dynamodb_1::to_item;

pub async fn write_items(
    items: &Vec<ItemModel>,
    ddb_client: &dynamo_db::Client,
) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError, HttpResponse>> {
    let item_reqs = items
        .iter()
        .map(|item_diff| to_item(item_diff).ok())
        .map(|payload| PutRequest::builder().set_item(payload).build().ok())
        .map(|req| WriteRequest::builder().set_put_request(req).build())
        .collect();

    // TODO: Failure handling:
    //       25 max, retries, ...

    ddb_client
        .batch_write_item()
        .request_items("items", item_reqs)
        .send()
        .await
}
