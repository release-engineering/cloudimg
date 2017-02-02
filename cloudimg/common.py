import logging
import os

import requests
from libcloud.storage.types import ContainerDoesNotExistError

log = logging.getLogger(__name__)


class BaseService(object):
    """
    Base class for all cloud provider services. All subclasses must implement
    the following methods:

        upload

    Args:
        storage: An instance of the service's libcloud storage driver
        compute: An instance of the service's libcloud compute driver
    """

    def __init__(self, storage, compute):
        self.storage = storage
        self.compute = compute

        # Default chunk size for streaming images from a remote URL; 4MB
        self.chunk_size = 4 * 1024 * 1024

    def upload(self, image, name=None):
        """
        Uploads an image with a given optional name.

        Args:
            image (str): A file path or URL to the image
            name (str, optional): An alternative name for the destination image
        """
        raise NotImplementedError('upload() must be implemented by subclasses')

    def upload_to_container(self, image, container_name, name=None):
        """
        Uploads an image to a storage container. If the image is a remote URL,
        it will be requested using chunk sizes determined by `self.chunk_size`.

        Args:
            image (str): A file path or URL to the image
            container_name (str): The name of the storage container
            name (str, optional): An override name for the destination image.
                By default, the original filename will be used.
        """
        log.info('Uploading %s to container %s', image, container_name)

        # Override the upload name if desired
        if not name:
            name = os.path.basename(image)

        log.info('Uploading %s with name %s', image, name)

        # Get or create the container
        try:
            container = self.storage.get_container(container_name)
        except ContainerDoesNotExistError:
            log.info('Creating container: %s', container_name)
            container = self.storage.create_container(container_name)

        if image.lower().startswith('http'):
            # Stream the upload from a remote URL
            log.info('Opening stream to: %s', image)
            resp = requests.get(image, stream=True)
            resp.raise_for_status()
            stream = resp.iter_content(self.chunk_size)
            self.storage.upload_object_via_stream(stream, container, name)
        else:
            # Upload a local file
            self.storage.upload_object(image, container, name)

        log.info('Successfully uploaded %s', image)
