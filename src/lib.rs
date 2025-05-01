use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_write_item::{BatchWriteItemError, BatchWriteItemOutput};
use aws_sdk_dynamodb::operation::put_item::{PutItemError, PutItemOutput};
use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};
use futures::future::join_all;
use item_core::item_model::ItemModel;
use serde_dynamo::aws_sdk_dynamodb_1::to_item;

const MAX_BATCH_SIZE: usize = 25;

/// Writes a single item to the DynamoDB abstracted by the given client.
pub async fn write_item(
    item: &ItemModel,
    ddb_client: &Client,
) -> Result<PutItemOutput, SdkError<PutItemError, HttpResponse>> {
    ddb_client
        .put_item()
        .table_name("items")
        .set_item(to_item(item).ok())
        .send()
        .await
}

/// Writes multiple items by splitting them into batches and writes these batches
/// to the DynamoDB abstracted by the given client.
pub async fn write_items(
    items: &[ItemModel],
    ddb_client: &Client,
) -> Vec<Result<BatchWriteItemOutput, SdkError<BatchWriteItemError, HttpResponse>>> {
    let reqs: Vec<_> = items
        .chunks(MAX_BATCH_SIZE)
        .map(|chunk| {
            chunk
                .iter()
                .map(|item_diff| to_item(item_diff).ok())
                .map(|payload| PutRequest::builder().set_item(payload).build().ok())
                .map(|req| WriteRequest::builder().set_put_request(req).build())
                .collect()
        })
        .map(|reqs| {
            ddb_client
                .batch_write_item()
                .request_items("items", reqs)
                .send()
        })
        .collect();

    join_all(reqs).await
}

/// Puts multiple items in a single batch and writes it to the DynamoDB abstracted by the given client.
pub async fn write_item_batch(
    items: &[ItemModel],
    ddb_client: &Client,
) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError, HttpResponse>> {
    let reqs = items
        .iter()
        .map(|item_diff| to_item(item_diff).ok())
        .map(|payload| PutRequest::builder().set_item(payload).build().ok())
        .map(|req| WriteRequest::builder().set_put_request(req).build())
        .collect();

    ddb_client
        .batch_write_item()
        .request_items("items", reqs)
        .send()
        .await
}
