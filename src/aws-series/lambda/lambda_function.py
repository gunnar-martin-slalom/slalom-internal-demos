"""

URL:
https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9

API:
https://data.cityofnewyork.us/resource/erm2-nwe9.json

https://data.cityofnewyork.us/resource/erm2-nwe9.json?$where=closed_date between '2023-10-21T00:00:00.000' and '2023-10-21T23:59:59.999'

https://dev.socrata.com/docs/functions/#,
https://dev.socrata.com/docs/paging.html

"""
import csv
import json
import urllib3


class BatchLoader:

    def __init__(
            self,
            base_path,
            run_date,
            offset_value,
            batch_size,
    ):

        self._base_path = base_path
        self._run_date = run_date
        self._offset_value = offset_value
        self._batch_size = batch_size
        self._url_path = None
        self._record_list = None
        self._http_pool_manager = None

    def _get_http_pool_manager(self):

        if self._http_pool_manager:
            return self._http_pool_manager

        # Creating a PoolManager instance for sending requests.
        self._http_pool_manager = urllib3.PoolManager()
        return self._http_pool_manager

    @property
    def url_path(self):

        self._url_path = str(
            f"https://data.cityofnewyork.us/resource/erm2-nwe9.json"
            f"?$where=closed_date between "
            f"'2023-10-21T00:00:00.000'"
            f" and "
            f"'2023-10-21T23:59:59.999'"
            f"&$limit={self._b}"
            f"&$offset=7077"
        )

        return self._url_path

    @property
    def record_list(self):

        if self._record_list:
            return self._record_list

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

    def _write_out_csv(self):

        csv_file_object = open("test_file.csv", "w", newline="")
        csv_writer = csv.writer(csv_file_object)

        count = 0
        for data in self.record_list:
            if count == 0:
                header = data.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(data.values())

        csv_file_object.close()


    def execute(self):

        self._write_out_csv()

        print(len(self.record_list))


class DataLoader:

    def __init__(
            self,
            batch_size=10,
    ):

        self._base_path = None
        self._batch_size = batch_size

    @property
    def base_path(self):
        """

        Info on this dataset:
            - https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9

        :return:
        """

        self._base_path = str(
            "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
            "?$where=closed_date between "
            "'2023-10-21T00:00:00.000'"
            " and "
            "'2023-10-21T23:59:59.999'"
            "&$limit=100"
            "&$offset=7077"
        )

        return self._base_path



    def execute(self):

        url_path = self.base_path

        offset = 0
        batch_size = 0

        while True:

            loader = BatchLoader(
                url_path=url_path
            )
            if len(loader.record_list) == 0:
                break

            loader.execute()

            print("Done.")

def mock_function():


    # Creating a PoolManager instance for sending requests.
    http = urllib3.PoolManager()

    url_path = "https://data.cityofnewyork.us/resource/erm2-nwe9.json?unique_key=59171877"
    url_path = "https://data.cityofnewyork.us/resource/erm2-nwe9.json?$where=closed_date between '2023-10-21T00:00:00.000' and '2023-10-21T23:59:59.999'&$limit=5"

    # Sending a GET request and getting back response as HTTPResponse object.
    resp = http.request("GET", url_path)

    # Print the returned data.
    print(resp.status)
    print(resp.data)

def lambda_handler(event, context):

    loader = DataLoader()

    loader.execute()


    return {
        "statusCode": 200,
        "body": json.dumps("Finished with success.")
    }


if __name__ == "__main__":
    lambda_handler(None, None)
