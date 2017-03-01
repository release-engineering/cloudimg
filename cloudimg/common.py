import logging
import os

import requests
from libcloud.storage.types import (ContainerDoesNotExistError,
                                    ObjectDoesNotExistError)

log = logging.getLogger(__name__)


class PublishingMetadata(object):
    """
    A collection of metadata necessary for uploading and publishing a disk
    image to a cloud provider.

    Args:
        image_path (str): A file path or URL to the image
        image_name (str): The name of the image. Used as a primary identifier.
        description (str, optional): The description of the image
        container (str, optional): The name of the storage container for
                                   uploads
        arch (str, optional): Ex. x86_64, i386
        virt_type (str, optional): Ex. hvm, paravirtual
        root_device_name (str, optional): Ex. /dev/sda1
        volume_type (str, optional): Ex. standard, gp2
        accounts (list, optional): Accounts which will have permission to use
                                   the image
        groups (list, optional): Groups which will have permission to use the
                                 image
    """

    def __init__(self, image_path, image_name, description=None,
                 container=None, arch=None, virt_type=None,
                 root_device_name=None, volume_type=None,
                 accounts=[], groups=[]):
        self.image_path = image_path
        self.image_name = image_name
        self.description = description
        self.container = container
        self.arch = arch
        self.virt_type = virt_type
        self.root_device_name = root_device_name
        self.volume_type = volume_type
        self.accounts = accounts
        self.groups = groups

    @property
    def snapshot_name(self):
        return os.path.splitext(self.object_name)[0]

    @property
    def object_name(self):
        return os.path.basename(self.image_path)


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
