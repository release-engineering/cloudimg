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

    def get_image(self, metadata):
        """
        Finds an image associated with some publishing metadata.

        Args:
            metadata: metadata about the disk image and how it should be
                      published

        Returns:
            A libcloud compute Image object if found; None otherwise
        """
        for image in self.compute.list_images(ex_owner='self'):
            if image.name == metadata.image_name:
                return image
        return None

    def get_object(self, metadata):
        """
        Finds a storage object associated with some publishing metadata.

        Args:
            metadata: metadata about the disk image and how it should be
                      published

        Returns:
            A libcloud storage Object if found; None otherwise
        """
        try:
            return self.storage.get_object(metadata.container,
                                           metadata.object_name)
        except ObjectDoesNotExistError:
            return None

    def get_snapshot(self, metadata):
        """
        Finds a snapshot associated with some publishing metadata.

        Args:
            metadata: metadata about the disk image and how it should be
                      published

        Returns:
            A libcloud compute VolumeSnapshot object if found; None otherwise
        """
        for snapshot in self.compute.list_snapshots(owner='self'):
            if snapshot.name == metadata.snapshot_name:
                return snapshot
        return None

    def publish(self, metadata):
        """
        Takes some metadata about a disk image, imports it into the cloud
        service and makes it available to others.

        Args:
            metadata: metadata about the disk image and how it should be
                      published

        Returns:
            A libcloud compute Image object
        """
        raise NotImplementedError('publish() must be implemented')

    def upload_to_container(self, metadata):
        """
        Uploads an image to a storage container. If the image is a remote URL,
        it will be requested using chunk sizes determined by `self.chunk_size`.

        Args:
            metadata: metadata about the disk image and how it should be
                      published

        Returns:
            A ``libcloud.storage.base.Object`` object
        """
        image_path = metadata.image_path
        container_name = metadata.container
        obj_name = metadata.object_name

        log.info('Uploading %s to container %s', image_path, container_name)
        log.info('Uploading %s with name %s', image_path, obj_name)

        # Get or create the container
        try:
            container = self.storage.get_container(container_name)
        except ContainerDoesNotExistError:
            log.info('Creating container: %s', container_name)
            container = self.storage.create_container(container_name)

        if image_path.lower().startswith('http'):
            # Stream the upload from a remote URL
            log.info('Opening stream to: %s', image_path)
            resp = requests.get(image_path, stream=True)
            resp.raise_for_status()
            stream = resp.iter_content(self.chunk_size)
            obj = self.storage.upload_object_via_stream(stream,
                                                        container,
                                                        obj_name)
        else:
            # We must upload as a stream to support multipart uploads for some
            # large files and/or providers (e.g. AWS)
            with open(image_path, 'rb') as stream:
                obj = self.storage.upload_object_via_stream(stream,
                                                            container,
                                                            obj_name)

        log.info('Successfully uploaded %s', image_path)

        return obj
