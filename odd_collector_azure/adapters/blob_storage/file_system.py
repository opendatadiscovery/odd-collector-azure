from typing import Union

import pyarrow.dataset as ds
from adlfs import AzureBlobFileSystem
from funcy import iffy, lmap
from pyarrow._fs import FileInfo, FileSelector

from odd_collector_azure.adapters.blob_storage.dataset_config import DatasetConfig
from odd_collector_azure.domain.plugin import BlobPlugin
from .domain.models import Container, File, Folder
from .logger import logger
from .utils import file_format


class FileSystem:
    """
    FileSystem hides pyarrow.fs implementation details.
    """

    def __init__(self, config: BlobPlugin):
        self.config = config
        params = {}
        if config.connection_string:
            params["connection_string"] = config.connection_string
        if not params.get("connection_string"):
            if config.account_name:
                params["account_name"] = config.account_name
            if config.account_key:
                params["account_key"] = config.account_key

        self.fs = AzureBlobFileSystem(**params)

    def get_file_info(self, path: str, file_filter) -> list[FileInfo]:
        """
        Get file info from path.
        @param path: blob path to file or folder
        @return: FileInfo
        """
        file_info = self.fs.ls(FileSelector(base_dir=path).base_dir, detail=True)
        filtered_file_info = list(filter(
            lambda obj: obj['type'] == 'directory' or file_filter(obj["name"].rsplit("/", 1)[-1]),
            file_info
        ))
        return filtered_file_info

    def get_dataset(self, file_path: str, format: str) -> ds.Dataset:
        """
        Get dataset from file path.
        @param file_path:
        @param format: Should be one of available formats: https://arrow.apache.org/docs/python/api/dataset.html#file-format
        @return: Dataset
        """
        return ds.dataset(source=file_path, filesystem=self.fs, format=format)

    def get_folder_as_file(self, dataset_config: DatasetConfig) -> File:
        """
        Get folder as Dataset.
        @param dataset_config:
        @return: File
        """
        logger.debug(f"Getting folder dataset for {dataset_config=}")

        dataset = ds.dataset(
            source=dataset_config.full_path,
            format=dataset_config.folder_as_dataset.file_format,
            partitioning=ds.partitioning(
                flavor=dataset_config.folder_as_dataset.flavor,
                field_names=dataset_config.folder_as_dataset.field_names,
            ),
            filesystem=self.fs,
        )
        return File(
            path=dataset_config.full_path,
            base_name=dataset_config.full_path,
            schema=dataset.schema,
            metadata={
                "Format": dataset_config.folder_as_dataset.file_format,
                "Partitioning": dataset_config.folder_as_dataset.flavor,
                "Flavor": dataset_config.folder_as_dataset.flavor,
                "FieldNames": dataset_config.folder_as_dataset.field_names,
            },
            format=dataset_config.folder_as_dataset.file_format,
        )

    def get_container(self, dataset_config: DatasetConfig) -> Container:
        """
        Get container with all related objects.
        @param dataset_config:
        @return: Container
        """
        container = Container(dataset_config.containers)
        if dataset_config.folder_as_dataset:
            container.objects.append(self.get_folder_as_file(dataset_config))
        else:
            objects = self.list_objects(
                path=dataset_config.full_path,
                file_filter=dataset_config.allow
            )
            container.objects.extend(objects)

        return container

    def list_objects(self, path: str, file_filter) -> list[Union[File, Folder]]:
        """
        Recursively get objects for path.
        @param path: blob path
        @return: list of either File or Folder
        """
        logger.debug(f"Getting objects for {path=}")
        return lmap(
            iffy(
                lambda x: True if x["type"] == "file" else False,
                lambda x: self.get_file(
                    x["name"],
                    x["name"].rsplit("/", 1)[-1] if x["type"] == "file" else None,
                ),
                lambda x: self.get_folder(x["name"], file_filter),
            ),
            self.get_file_info(path, file_filter)
        )

    def get_file(self, path: str, file_name: str = None) -> File:
        """
        Get File with schema and metadata.
        @param path: blob path to file
        @param file_name: file name
        @return: File
        """
        if not file_name:
            file_name = path.split("/")[-1]

        try:
            file_fmt = file_format(file_name)
            dataset = self.get_dataset(path, file_fmt)
            return File.dataset(
                path=path,
                name=file_name,
                schema=dataset.schema,
                file_format=file_fmt,
                metadata={},
            )
        except Exception as e:
            logger.warning(f"Failed to get schema for file {path}: {e}")
            return File.unknown(
                path=path,
                base_name=file_name,
                file_format="unknown",
            )

    def get_folder(self, path: str, file_filter, recursive: bool = True) -> Folder:
        """
        Get Folder with objects recursively.
        @param path: blob path to
        @return: Folder class with objects and path
        """
        objects = self.list_objects(path, file_filter) if recursive else []
        return Folder(path, objects)
