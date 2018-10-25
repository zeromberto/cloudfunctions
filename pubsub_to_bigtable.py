import base64
import json
from json import JSONDecodeError

from google.cloud import bigtable


def ingess(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
        Args:
             event (dict): Event payload.
             context (google.cloud.functions.Context): Metadata for the event.
     """
    client = bigtable.Client(project="mcp-iotcore-eval")
    instance = client.instance("mcp-iotcore-eval")
    table = instance.table("mm1")
    column_family_id = "observation"

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(context)
    print(pubsub_message)

    try:
        message: dict = json.loads(pubsub_message)

        # rows = []
        # for key, value in message.items():
        #     row_key = f''
        #     column = key.encode()
        #     row = table.row(row_key)
        #     row.set_cell(column_family_id,
        #                  column,
        #                  value)
        #     rows.append(row)
        # table.mutate_rows(rows)
    except JSONDecodeError:
        pass
