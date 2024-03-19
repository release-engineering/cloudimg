from datetime import datetime, timedelta
import logging
import lzma
import os

from time import monotonic
from urllib.parse import urlparse

import attr
from tenacity import (
    Retrying,
    stop_after_attempt,
    retry_if_not_result,
    stop_after_delay,
    wait_fixed,
)

from azure.core.exceptions import AzureError
from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient,
    BlobSasPermissions,
    BlobType,
    generate_account_sas,
    generate_blob_sas,
    ResourceTypes,
)

from cloudimg.common import BaseService, DeleteMetadata, PublishingMetadata

log = logging.getLogger(__name__)


class BlobNotFoundError(Exception):
    """
    Raised when a required Blob is missing.
    """
    pass


def _get_values_from_connection_string(connection_str):
    """
    Get all the values from the connection string.

    Args:
        connection_string (str): The connection string of the
                                 storage account

    Returns:
        A dictionary with all the key-values in the connection string
    """
    # Extraction
    res = {}
    val_list = connection_str.split(";")
    for val in val_list:
        if "=" in val:
            entry = val.split("=", 1)
            res[entry[0]] = entry[1]
        else:
            log.warning("Missing keyword assignment for the entry %s" % val)

    # Validation
    mandatory_keys = ["AccountName", "AccountKey"]
    err_prefix = "Invalid connection string:"
    if not res:
        raise ValueError("%s No keyword elements found." % err_prefix)

    for mkey in mandatory_keys:
        if mkey not in res.keys():
            raise ValueError("%s Missing key %s" % (err_prefix, mkey))
        elif not res.get(mkey, None):
            raise ValueError("%s Missing value for %s" % (err_prefix, mkey))

    return res


def _get_tags_filter_expression(tags):
    """
    Create a filter expression from the given tags for search.

    The filter expression is a string with format:
    ```
    "tag1"='value1' and "tag2"='value2' ...
    ```

    Args:
        tags (dict): Dictionary of strings with the tags to filter

    Returns:
        String with the filter expression.
    """

    # Create the filter expression like:
    # "tag1"='value1" and "tag2" = 'value2" ...
    return " and ".join(["\"%s\"='%s'" % (k, v) for k, v in tags.items()])


@attr.s
class UploadProgress(object):
    """
    Responsible for storing the upload progress.

    It's expected to be instantiated by a callback function during blob
    upload phase.

    Args:
        current (float): The current uploaded size in bytes
        total (float): The total size to upload in bytes
        percentage (float, optional): The upload progress percentage
    """

    current = attr.ib(type=float, repr=lambda x: "%.2fMB" % (x / 1024**2))
    total = attr.ib(type=float, repr=lambda x: "%.2fMB" % (x / 1024**2))
    percentage = attr.ib(type=float, repr=lambda x: "%.2f%%" % (x))
    last_seen = attr.ib(type=float, repr=False, eq=False, factory=monotonic)

    @percentage.default
    def _auto_percentage(self):
        if self.total > 0:
            return self.current / self.total * 100.0
        return 0


class AzurePublishingMetadata(PublishingMetadata):
    """
    A collection of metadata necessary for uploading and publishing a
    Virtual Hard Disk (VHD) to Azure.
    """

    def __init__(self, *args, **kwargs):
        super(AzurePublishingMetadata, self).__init__(*args, **kwargs)

        assert self.container, 'A container must be defined'
        assert self.tags, 'A tag must be defined'


class AzureDeleteMetadata(DeleteMetadata):
    """The required information to delete a blob on Azure."""

    def __init__(self, container, *args, **kwargs):
        # Set the image NAME as the ID for Azure as both are redundant
        # for this marketplace.
        if not kwargs.get("image_name"):
            kwargs.update({"image_name": kwargs.get("image_id")})

        super(AzureDeleteMetadata, self).__init__(*args, **kwargs)

        self.container = container


class AzureService(BaseService):
    """
    Azure cloud provider service.
    It's expected to be instantiated through one of its factories.

    Args:
        blob_service_client (BlobServiceClient): The blob service client to
                                                 manipulate the storage.
        storage_account_name (str): The storage account name for auth.
        storage_account_key (str): The storage account key for auth.
    """
    # The duration of generated SAS URIs
    SAS_EXPIRY = int(os.getenv("CLOUDIMG_AZURE_SAS_EXPIRY", 365 * 3))
    # The max concurrency for blob upload
    UPLOAD_MAX_CONCURRENCY = int(os.getenv(
                                "CLOUDIMG_AZURE_UPLOAD_MAX_CONCURRENCY",
                                16,
                             ))
    # The interval in seconds between showing upload progress in logs
    UPLOAD_LOG_INTERVAL = int(os.getenv("CLOUDIMG_AZURE_LOG_INTERVAL", 15))

    # The account URL suffix which comes after `https://{ACCOUNT_NAME}.`
    AZURE_STORAGE_ENDPOINT_SUFFIX = os.getenv(
                                        "AZURE_STORAGE_ENDPOINT_SUFFIX",
                                        "blob.core.windows.net"
                                    )

    def __init__(self, blob_service_client, storage_account_name,
                 storage_account_key, **kwargs):

        self.blob_service_client = blob_service_client
        self._account_name = storage_account_name
        self._account_key = storage_account_key
        self._upload_progress = UploadProgress(0, 0)
        self._upload_last_log = self._upload_progress.last_seen

        super(AzureService, self).__init__(**kwargs)

    @classmethod
    def from_connection_string(cls, connection_str):
        """
        Factory for AzureService using the storage connection string for auth.

        Args:
            connection_str (str): The storage connection string for auth.
        """
        values = _get_values_from_connection_string(connection_str)
        blob_service_client = BlobServiceClient.from_connection_string(
                                  connection_str
                              )
        return cls(
            blob_service_client=blob_service_client,
            storage_account_name=values['AccountName'],
            storage_account_key=values['AccountKey'],
        )

    @classmethod
    def from_storage_account(cls, account_name, account_key):
        """
        Factory for AzureService using the storage account name and key
        for auth.

        Args:
            account_name (str): The storage account name.
            account_key (str): The storage account key.
        Returns:
            AzureService object
        """
        sas_token = generate_account_sas(
            account_name=account_name,
            account_key=account_key,
            resource_types=ResourceTypes(
                service=True,
                container=True,
                object=True
            ),
            permission=AccountSasPermissions(
                read=True,
                write=True,
                list=True,
                add=True,
                create=True,
                update=True,
                process=True,
                tag=True,
                filter_by_tags=True,
            ),
            expiry=datetime.utcnow() + timedelta(days=cls.SAS_EXPIRY)
        )
        blob_service_client = BlobServiceClient(
            account_url="https://%s.%s" % (
                account_name,
                cls.AZURE_STORAGE_ENDPOINT_SUFFIX
            ),
            credential=sas_token
        )
        return cls(
            blob_service_client=blob_service_client,
            storage_account_name=account_name,
            storage_account_key=account_key,
        )

    def get_container_by_name(self, name, create=False, **kwargs):
        """
        Retrieve a container with the given name.
        If the container doesn't exist it will return `None` by default or
        create it when the `create` parameter is set explicitly set to `True`.

        Args:
            name (str): The name of the container to retrieve/create
            create (bool): Whether to create the container if it doesn't exist
        Returns
            The requested ContainerClient if it exists
        """
        container_client = self.blob_service_client.get_container_client(name)
        if not container_client.exists():
            log.info("The requested container \"%s\" doesn't exist." % name)
            if not create:
                log.info("Skipping container creation for %s" % name)
                return
            log.info('Creating container: %s', name)
            container_client.create_container(**kwargs)
        return container_client

    def get_object_by_name(self, container, name):
        """
        Finds an object with a given name in a given container.

        Args:
            container (str): The name of the container
            name (str): The name of the image

        Returns:
            A BlobClient if found; None otherwise
        """
        container_client = self.get_container_by_name(name=container)
        if container_client:
            blob = container_client.get_blob_client(blob=name)
            return blob if blob.exists() else None
        return

    def filter_object_by_tags(self, tags):
        """
        Check if a blob exists in any container from the given account.

        Args:
            tags (dict): The tags to search for
        Returns:
            FilteredBlob with the result or None when not found.
        """
        filter_expression = _get_tags_filter_expression(tags)

        # Search for the given tags in each container on storage account
        for cprops in self.blob_service_client.list_containers():
            container_client = self.get_container_by_name(name=cprops.name)
            if container_client:
                blobs_list = container_client.find_blobs_by_tags(
                    filter_expression=filter_expression
                )
                try:
                    return blobs_list.next()
                except StopIteration:
                    continue
        return

    @staticmethod
    def are_tags_present(container_client, tags):
        """
        Check if a blob with the given tags already exists in the specific
        container in the present storage account.

        Args:
            container_client (ContainerClient): The container client to check
                                                the tags against of
            tags (dict): The tags to check for existence
        Returns:
            True if the tags are present, False otherwise
        """
        result = True
        filter_expression = _get_tags_filter_expression(tags)
        blobs_list = container_client.find_blobs_by_tags(
            filter_expression=filter_expression
        )
        try:
            blobs_list.next()
        except StopIteration:
            result = False
        return result

    def upload_callback(self, response):
        """
        Callback for logging the upload progress.

        Args:
            response (Dict[str, Any]): Dictionary with the upload progress
                                       information.
        Returns:
            UploadProgress object representing the upload progress.
        """
        current = response.context['upload_stream_current']
        total = response.context['data_stream_total']

        prev = self._upload_progress
        now = monotonic()
        show = now - self._upload_last_log >= self.UPLOAD_LOG_INTERVAL

        if current is not None and current > prev.current:
            self._upload_progress = UploadProgress(current, total)

        if show:
            self._upload_last_log = self._upload_progress.last_seen
            log.info(str(self._upload_progress))

        return self._upload_progress

    def _get_blob_copy_status(self, blob_client):
        """
        Wait  and confirm till the copy of blob from url is completed.

        Args:
            blob_client : Blob client which initiated the copy process.
        Returns:
            Upload blob client on success
        Raises:
            AzureError on upload failure.
        """
        get_copy_progess = (
            lambda uploaded, total: (int(uploaded) / int(total))*100
        )

        try:
            copy_status = (
                lambda blob_properties:
                blob_properties["copy"]["status"] == "success"
            )
            for attempt in Retrying(
                stop=stop_after_attempt(60) | stop_after_delay(600),
                retry=retry_if_not_result(copy_status),
                wait=wait_fixed(10),
            ):
                with attempt:
                    blob_properties = blob_client.get_blob_properties()
                attempt.retry_state.set_result(blob_properties)
                copy_status = blob_properties["copy"]["status"]
                copy_progress = round(
                    get_copy_progess(
                        *blob_properties["copy"]["progress"].split("/")), 2
                )
                log.info(f"Copying in progress : {copy_progress} %")
            return blob_client
        except Exception as e:
            log.error(
                f"Unable to confirm if the blob was copied successfully: {e}")
            raise AzureError(e)

    def upload_to_container(self, image_path, container_name, object_name,
                            tags, **kwargs):
        """
        Upload a blob to a container in a storage account and tag it.
        If the file_url starts with http, use the upload_blob_from_url method,
        otherwise use the upload_blob method.

        Args:
            image_path (str): Local or remote path to the image
            container_name (str): The container to upload to
            object_name (str): The uploaded file name
            tags (dict): Tags to apply to the uploaded image
            **kwargs: Additional arguments for upload_blob and
                      upload_blob_from_url

        Returns:
            The uploaded BlobClient on success.
        Raises:
            AzureError on upload failure.
        """
        # Initial parameters
        self._upload_progress = UploadProgress(current=0, total=0)
        self._upload_last_log = self._upload_progress.last_seen
        upload_default_args = {
            "tags": tags,
            "max_concurrency": self.UPLOAD_MAX_CONCURRENCY,
            "raw_response_hook": self.upload_callback,
        }
        for k, v in upload_default_args.items():
            kwargs.setdefault(k, v)  # noqa: E731

        # Setup the destination container
        container_client = self.get_container_by_name(
            name=container_name,
            create=True
        )

        # Azure can't handle compressed images on marketplaces so we need to
        # send the decompressed data to its storage account.
        if image_path.endswith(".xz"):
            log.info("Processing a LZMA compressed file: %s.",
                     os.path.basename(image_path))
            open_func = lzma.open
        else:
            open_func = open

        # Check the image tags
        log.info("Checking whether the image was already uploaded.")
        if self.are_tags_present(container_client, tags):
            log.info(
                "The tags '%s' already exists in storage account "
                "'%s' under container "
                "'%s' \n"
                "thus the image will not be uploaded." %
                (
                    tags,
                    container_client.account_name,
                    container_client.container_name
                )
            )
            filtered = self.filter_object_by_tags(tags)
            return self.get_object_by_name(
                        container=filtered.container_name,
                        name=filtered.name,
                   )

        # Upload to container and tag image
        blob_client = container_client.get_blob_client(blob=object_name)
        if blob_client.exists():
            log.info(
                "The image \"%s\" already exists in container \"%s\"",
                object_name,
                container_name,
            )
            return self.get_object_by_name(
                        container=container_name,
                        name=object_name,
                   )

        is_image_path_an_url = lambda urlparse_result: all(     # noqa: E731
            [urlparse_result.scheme, urlparse_result.netloc]
        )

        if is_image_path_an_url(urlparse(image_path)):
            log.info('Copying %s to container %s', image_path, container_name)
            blob_client.start_copy_from_url(
                source_url=image_path, metadata={}, incremental_copy=False
            )
            return self._get_blob_copy_status(blob_client)

        log.info('Uploading %s to container %s', image_path, container_name)
        log.info('Uploading %s with name %s', image_path, object_name)
        with open_func(image_path, "rb") as data:
            # we need to pass the lenght to upload_blob as it will try to
            # guess the length of LZMAFile and it get the incorrect value,
            # which is the size of compressed file.
            bytes_count = data.seek(0, os.SEEK_END)
            data.seek(0)
            log.debug("Upload size: %d bytes", bytes_count)
            blob_client.upload_blob(
                data=data,
                blob_type=BlobType.PAGEBLOB,
                length=bytes_count,
                **kwargs
            )
        log.info(str(self._upload_progress))
        log.info('Successfully uploaded %s', image_path)
        return blob_client

    def publish(self, metadata):
        """
        Take some metadata about a VHD image and upload it to Azure.

        This method relies on unique image/metadata names in order to do the
        least amount of work possible.

        If the incoming ``metadata.image_name`` is compressed as ``.xz`` its
        content will be automatically decompressed before uploading.

        Args:
            metadata (AzurePublishingMetadata): Metadata about the VHD image

        Returns:
            The BlobProperties with the data from expected blob.
        """
        log.info(
            'Searching for image: %s in container %s',
            metadata.object_name,
            metadata.container,
        )
        blob = self.get_object_by_name(
                    container=metadata.container,
                    name=metadata.object_name,
                )

        if not blob:
            log.info('Image does not exist: %s', metadata.object_name)
            log.info('Searching for tags: %s', metadata.tags)

            filtered_blob = self.filter_object_by_tags(metadata.tags)

            if not filtered_blob:
                log.error("Image not found with tags \"%s\"", metadata.tags)
                blob = self.upload_to_container(
                    image_path=metadata.image_path,
                    container_name=metadata.container,
                    object_name=metadata.object_name,
                    tags=metadata.tags,
                )
            else:
                log.info(
                    "Image already exists on container \"%s\""
                    " with name \"%s\"",
                    filtered_blob.container_name,
                    filtered_blob.name,
                )

                blob = self.get_object_by_name(
                    container=filtered_blob.container_name,
                    name=filtered_blob.name,
                )

        else:
            log.info("Image already exists with name \"%s\"", blob.blob_name)

        log.info('Image published: %s/%s', blob.container_name, blob.blob_name)
        return blob.get_blob_properties()

    def get_blob_sas_uri(self, blob_props):
        """
        Get a SAS URL of a blob using its Storage account's connection string.
        The SAS URL is with read-only permissions that will expire
        in `self.SAS_EXPIRY` day(s).

        Args:
            container_name (str): The blob's container name
            blob_props (BlobProperties): The uploaded blob properties
        Returns:
            The blob's full SAS URI
        """
        logging.info(
            "Generating the SAS URI for %s/%s",
            blob_props.container,
            blob_props.name,
        )
        uri = generate_blob_sas(
            account_name=self._account_name,
            account_key=self._account_key,
            container_name=blob_props.container,
            blob_name=blob_props.name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(days=self.SAS_EXPIRY),
        )
        # Full SAS format:
        # https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview#sas-token
        full_sas_uri = (
            "https://%s.%s/%s/%s?%s" % (
                self._account_name,
                self.AZURE_STORAGE_ENDPOINT_SUFFIX,
                blob_props.container,
                blob_props.name,
                uri,
            )
        )
        return full_sas_uri

    def tag_image(self, blob_props, tags):
        """
        Apply the corresponding tags to an uploaded blob
        Args:
            blob_props (BlobProperties): The uploaded blob properties
            tags (dict): Dictionary with the tags to apply
        Returns:
           Dictionary with updated properties
        """
        blob = self.get_object_by_name(
                    container=blob_props.container,
                    name=blob_props.name
               )

        if not blob:
            err = "Attempting to tag a missing blob: %s/%s." % (
                      blob_props.container,
                      blob_props.name
                  )
            log.error(err)
            raise BlobNotFoundError(err)

        return blob.set_blob_tags(tags)

    def delete(self, metadata):
        """
        Delete VHD images for given metadata.

        Args:
            metadata(AzureDeleteMetadata): Metadata about the image

        Returns:
            tuple (image_name[Str], image_path[Str]) of removed objects.
        """
        image_name = metadata.image_name

        log.info('Searching for image: %s', image_name)
        blob = self.get_object_by_name(
            container=metadata.container,
            name=image_name
        )

        if not blob:
            log.info('Image does not exist: %s', image_name)
            return None, None

        image_path = "%s/%s" % (blob.container_name, blob.blob_name)
        log.info("Deleting the image from %s", image_path)
        # https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python#azure-storage-blob-blobclient-delete-blob
        blob.delete_blob(delete_snapshots="include")

        log.info("Deleted the image from %s", image_path)
        return image_name, image_path
