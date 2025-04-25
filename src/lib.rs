use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::{PutItemError, PutItemOutput};
use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};
use item_core::item_model::ItemModel;
use serde_dynamo::aws_sdk_dynamodb_1::to_item;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

const MAX_BATCH_SIZE: usize = 25;
const MAX_RETRIES: usize = 5;

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

// TODO: Return
//  Result<(), Vec<(&'items ItemModel, WriteError)>>
//  where WriteError encapsulates what went wrong: Retries exceeded, SdkError, to_item error, ...
pub async fn write_items<'items>(
    items: &'items [ItemModel],
    ddb_client: &Client,
) -> Result<(), Vec<(&'items ItemModel, String)>> {
    let mut failed_items = Vec::new();

    let item_reqs: Vec<(&ItemModel, WriteRequest)> = items
        .iter()
        .map(|model| {
            let item = to_item(model)
                .expect("'serde_dynamo::aws_sdk_dynamodb_1::to_item' won't fail due to extensive and pedantic testing");
            let put_req = PutRequest::builder()
                .set_item(Some(item))
                .build()
                .expect("cannot fail because 'PutRequestBuilder'::set_item' has been set");
            let write_req = WriteRequest::builder()
                .put_request(put_req)
                .build();
            (model, write_req)
        })
        .collect();

    // DynamoDB can only handle batch-writes up to size 25
    for chunk in item_reqs.chunks(MAX_BATCH_SIZE) {
        let mut remaining = chunk.to_vec();

        let mut attempt = 0;
        while !remaining.is_empty() && attempt < MAX_RETRIES {
            let write_result = ddb_client
                .batch_write_item()
                .request_items(
                    "items",
                    remaining.iter().map(|(_, req)| req.to_owned()).collect(),
                )
                .send()
                .await;

            match write_result {
                Ok(output) => {
                    let def_unprocessed = HashMap::new();
                    let unprocessed = output.unprocessed_items().unwrap_or(&def_unprocessed);
                    match unprocessed.get("items") {
                        Some(unprocessed_writes) => {
                            let still_remaining = unprocessed_writes
                                .iter()
                                .filter_map(|unproc_req| {
                                    // Match unprocessed requests back to original items
                                    chunk.iter().find(|(_, req)| req == unproc_req).cloned()
                                })
                                .collect::<Vec<(&ItemModel, WriteRequest)>>();

                            remaining = still_remaining;
                        }
                        None => remaining.clear(),
                    }
                }
                Err(e) => {
                    // TODO: We need more extensive error handling here
                    //       Handle all the different cases explicitly and consider retrying where possible only
                    //       Only then return those not possible
                    failed_items.extend(
                        remaining
                            .iter()
                            .map(|(item, _)| (*item, format!("Batch write error: {}", e))),
                    );
                    remaining.clear();
                    break;
                }
            }

            if !remaining.is_empty() {
                attempt += 1;
                sleep(Duration::from_millis(100 * 2u64.pow(attempt as u32))).await;
            }
        }

        // After retries, still remaining
        failed_items.extend(
            remaining
                .iter()
                .map(|(item, _)| (*item, format!("Unprocessed after {} retries", MAX_RETRIES))),
        )
    }

    if failed_items.is_empty() {
        Ok(())
    } else {
        Err(failed_items)
    }
}
