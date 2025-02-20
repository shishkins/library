import os
from io import BytesIO
from pathlib import Path
from typing import Any
from typing import BinaryIO

import boto3
import pandas as pd
from botocore.client import Config


class S3:
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str = None,
        config: Config = None,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.config = config

    def __get_bucket(self, bucket: str) -> Any:
        bucket = bucket or self.bucket
        if not bucket:
            raise ValueError('Bucket is not specified. Set the "bucket" parameter.')
        return self.get_connection().Bucket(bucket)

    def get_connection(self, **kwargs) -> Any:
        """
        Get connection to S3
        """

        return boto3.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=self.config,
            **kwargs
        )

    def get_buckets(self) -> tuple:
        """
        Get tuple of buckets
        """

        return tuple(bucket.name for bucket in self.get_connection().buckets.all())

    def get_objects(
        self,
        prefix: str = None,
        bucket: str = None,
    ) -> tuple:
        """
        Get tuple of objects in bucket

        :param prefix: Get objects with prefix. Optional
        :param bucket: Bucket name. Optional
        """

        if prefix:
            return tuple(
                obj.key
                for obj in self.__get_bucket(bucket).objects.filter(Prefix=prefix).all()
            )
        else:
            return tuple(obj.key for obj in self.__get_bucket(bucket).objects.all())

    def upload(
        self,
        file: BytesIO | BinaryIO | Path | str,
        object_name: str,
        bucket: str = None,
    ) -> None:
        """
        Upload file to S3

        :param file: File-like object or path to local file
        :param object_name: Object name in S3. Example: path/to/file
        :param bucket: Bucket name. Optional
        """

        if isinstance(file, (Path, str)):
            self.__get_bucket(bucket).upload_file(file, object_name)
        else:
            self.__get_bucket(bucket).Object(object_name).put(Body=file)

    def upload_df(
        self,
        df: pd.DataFrame,
        object_name: str,
        bucket: str = None,
    ) -> None:
        """
        Upload DataFrame to S3. Supported formats: csv, xlsx

        :param df: DataFrame
        :param object_name: Object name in S3. Example: path/to/file
        :param bucket: Bucket name. Optional
        """

        buffer = BytesIO()
        _, file_type = os.path.splitext(object_name)

        match file_type:
            case '.csv':
                df.to_csv(buffer, index=False)
            case '.xlsx':
                df.to_excel(buffer, index=False)
            case _:
                raise ValueError(f"Not supported file type: {file_type}")

        buffer.seek(0)
        self.upload(file=buffer, object_name=object_name, bucket=bucket)

    def download(
        self,
        object_name: str,
        path: Path | str = None,
        bucket: str = None,
    ) -> BytesIO | None:
        """
        Download file from S3

        :param object_name: Object name in S3. Example: path/to/file
        :param path: Local path to save file. If not specified, return file-like object
        :param bucket: Bucket name. Optional
        """

        if path:
            self.__get_bucket(bucket).download_file(object_name, path)
        else:
            file = self.__get_bucket(bucket).Object(object_name).get()["Body"].read()
            return BytesIO(file)

    def download_df(
        self,
        object_name: str,
        bucket: str = None,
    ) -> pd.DataFrame:
        """
        Download file as DataFrame from S3. Supported formats: csv, xlsx, xls

        :param object_name: Object name in S3. Example: path/to/file
        :param bucket: Bucket name. Optional
        """
        _, file_type = os.path.splitext(object_name)
        file = self.download(object_name=object_name, bucket=bucket)

        match file_type:
            case '.csv':
                return pd.read_csv(file)
            case '.xlsx':
                return pd.read_excel(file, engine='openpyxl')
            case _:
                raise ValueError(f"Not supported file type: {file_type}")

    def delete(
        self,
        object_name: str,
        bucket: str = None,
    ) -> None:
        """
        Delete file from S3

        :param object_name: Object name in S3. Example: path/to/file
        :param bucket: Bucket name. Optional
        """

        self.__get_bucket(bucket).Object(object_name).delete()
