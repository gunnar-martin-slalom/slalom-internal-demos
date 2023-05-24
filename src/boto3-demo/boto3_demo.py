import json
import boto3
import datetime


class DateTimeEncoder(json.JSONEncoder):
    """
    Override the default method of JSON encoding for datetime
    source: https://pynative.com/python-serialize-datetime-into-json/

    Used for saving boto3 responses to JSON for posterity
    """

    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


def lambda_handler(event, context):

    # -- Check for demo ID --
    demo_id = event.get("demo_id", None)
    if not demo_id:
        msg = "Invalid event. Key `demo_id` not found in event."
        raise Exception(msg)

    # -- Instantiate the session and client --

    # Insert a profile name here if running locally
    session = boto3.session.Session()

    # -- Run a demo function --

    if demo_id == "demo_01":

        s3_client = session.client("s3")
        print("Calling list objects...")
        response = s3_client.list_objects_v2(
            Bucket="atl-airport-faqs-test",
        )
        print(json.dumps(response, cls=DateTimeEncoder))

    elif demo_id == "demo_02":

        sts_client = session.client("sts")
        print("Calling get caller ID...")
        response = sts_client.get_caller_identity()
        print(json.dumps(response, cls=DateTimeEncoder))

    elif demo_id == "demo_03":

        s3_client = session.client("s3")

        obj_body = b"Hello from demo #3!"

        print("Putting an object")
        response = s3_client.put_object(
            Bucket="atl-airport-faqs-test",
            Key="atl-boto3-demo/new_object.txt",
            Body=obj_body,
        )
        print(json.dumps(response, cls=DateTimeEncoder))

    return {
        "statusCode": 200,
        "body": "Lambda Done!"
    }
