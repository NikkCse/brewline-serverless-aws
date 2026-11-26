"""Invoked by a one-time EventBridge Scheduler timer: READY -> ABANDONED if nobody picked it up.
The conditional update makes late or duplicate timers harmless."""
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

orders_table = boto3.resource("dynamodb").Table(os.environ["ORDERS_TABLE"])
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    order_id = event.get("orderId")
    if not order_id:
        logger.warning("No orderId in event - ignoring")
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    try:
        orders_table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET #s = :abandoned, updatedAt = :now",
            ConditionExpression="#s = :ready",  # only if nobody picked it up
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":abandoned": "ABANDONED", ":ready": "READY", ":now": now},
        )
        logger.info("Order %s ABANDONED - not picked up in time", order_id)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("Order %s no longer READY - nothing to do", order_id)
            return
        raise
