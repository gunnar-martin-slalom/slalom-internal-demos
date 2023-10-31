"""
Loads a single `closed_date` to s3 as CSV files.
"""
import boto3
import csv
import datetime
import io
import json
import urllib3


class BatchLoader:

    def __init__(
            self,
            run_date,
            offset_value,
            limit_value,
    ):

        self._run_date = run_date
        self._offset_value = offset_value
        self._limit_value = limit_value
        self._url_path = None
        self._record_list = None
        self._http_pool_manager = None
        self._s3_object_key = None

    def _get_http_pool_manager(self):

        if self._http_pool_manager:
            return self._http_pool_manager

        # Creating a PoolManager instance for sending requests.
        self._http_pool_manager = urllib3.PoolManager()
        return self._http_pool_manager

    @property
    def s3_object_bucket(self):
        return "aws-series-s3-demo-0001"

    @property
    def s3_object_key(self):
        """
        :return:
            Object key for the CSV file.

            Ex: ".../data-00007765.csv"
        """

        offset_fill = str(self.offset_value).zfill(8)

        s3_object_key = str(
            f"demo-folder-lambda"
            f"/run-date"
            f"/{self.run_date}"
            f"/data-{offset_fill}.csv"
        )

        return s3_object_key

    @property
    def run_date(self):
        return self._run_date

    @property
    def offset_value(self):
        return self._offset_value

    @property
    def limit_value(self):
        return self._limit_value

    @property
    def url_path(self):

        self._url_path = str(
            f"https://data.cityofnewyork.us/resource/erm2-nwe9.json"
            f"?$where=closed_date between "
            f"'{self.run_date}T00:00:00.000'"
            f" and "
            f"'{self.run_date}T23:59:59.999'"
            f"&$limit={self.limit_value}"
            f"&$offset={self.offset_value}"
        )

        return self._url_path

    @property
    def record_list(self):
        """
        :return:
            List of Python dictionaries containing data.
        """

        if self._record_list:
            return self._record_list

        print(f" Getting dat from path: {self.url_path}")

        # Sending a GET request and getting back response as HTTPResponse object.
        http = self._get_http_pool_manager()
        resp = http.request("GET", self.url_path)

        if resp.status != 200:
            msg = "Something went wrong. Status not 200."
            raise Exception(msg)

        # Convert to string
        resp_str = resp.data.decode("utf-8")

        # Convert to a list of Python dictionaries
        self._record_list = json.loads(resp_str)

        # Remove "location" key from the record
        for record in self._record_list:
            record.pop("location", None)

        return self._record_list

    def execute(self):

        # String buffer to write to
        csv_string_buffer = io.StringIO()
        
        # Instantiate CSV writer object
        csv_writer = csv.writer(csv_string_buffer)

        count = 0
        for data in self.record_list:
            if count == 0:
                header = data.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(data.values())

        # Convert the buffer to binary for put_object() method
        binary_data = csv_string_buffer.getvalue().encode("utf-8")

        msg = str(
            f"Writing data to: s3://{self.s3_object_bucket}"
            f"/{self.s3_object_key}"
        )
        print(msg)

        # Put object to s3
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Body=binary_data,
            Bucket=self.s3_object_bucket,
            Key=self.s3_object_key,
        )

        # Close the buffer
        csv_string_buffer.close()


class DataLoader:

    def __init__(
            self,
            batch_size,
            run_date=None,
    ):

        self._batch_size = batch_size
        self._run_date = run_date

    @property
    def run_date(self):
        """
        If `run_date` is not given in the instantiation of DataLoader,
          this will default to the date 7 days prior to today

        :return:
            String like "YYYY-MM-DD" denoting the date
        """

        if self._run_date:
            return self._run_date

        days_prior_ct = 7
        now_dt = datetime.datetime.now()
        past_dt = now_dt + datetime.timedelta(days=-days_prior_ct)
        self._run_date = past_dt.strftime("%Y-%m-%d")

        return self._run_date

    @property
    def batch_size(self):
        return self._batch_size

    def execute(self):
        """
        Executions instances of BatchLoader() until every page has been
          output to s3.

        :return: None
        """

        curr_offset_value = 0

        while True:

            print(f"curr_offset_value: {curr_offset_value}")

            loader = BatchLoader(
                run_date=self.run_date,
                offset_value=curr_offset_value,
                limit_value=self.batch_size,
            )

            print(f"Record count: {len(loader.record_list)}")

            if len(loader.record_list) == 0:
                break

            loader.execute()

            # Increment offset
            curr_offset_value += self.batch_size

        print("Done.")

def lambda_handler(event, context):

    loader = DataLoader(
        batch_size=500,
        run_date=event.get("run_date"),
    )
    loader.execute()

    return {
        "statusCode": 200,
        "body": json.dumps("Finished with success.")
    }

# For running script locally
if __name__ == "__main__":
    lambda_handler({}, None)
