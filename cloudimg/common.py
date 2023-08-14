from abc import ABCMeta, abstractmethod
import logging
import os

log = logging.getLogger(__name__)


class PublishingMetadata(object):
    """
    A collection of metadata necessary for uploading and publishing a disk
    image to a cloud provider.

    Args:
        image_path (str): A file path or URL to the image
        image_name (str): The name of the image. Used as a primary identifier.
        snapshot_name (str): The name of the snapshot. Derived from the image
                             filename by default.
        description (str, optional): The description of the image
        container (str, optional): The name of the storage container for
                                   uploads
        arch (str, optional): Ex. x86_64, i386
        virt_type (str, optional): Ex. hvm, paravirtual
        root_device_name (str, optional): Ex. /dev/sda1
        volume_type (str, optional): Ex. standard, gp2
        uefi_support (Bool, optional): Whether the image supports UEFI boot
        accounts (list, optional): Accounts which will have permission to use
                                   the image
        groups (list, optional): Groups which will have permission to use the
                                 image
        snapshot_account_ids (list, optional): Accounts which will have
                                            permission to create the snapshot
        tags (dict, optional): Tags to be applied to the image.
    """

    def __init__(self, image_path, image_name, description=None,
                 container=None, arch=None, virt_type=None,
                 root_device_name=None, volume_type=None,
                 accounts=[], groups=[], snapshot_name=None,
                 snapshot_account_ids=None, tags=None, uefi_support=None):
        self.image_path = image_path
        self.image_name = image_name
        self.snapshot_name = snapshot_name or self._default_snapshot_name
        self.snapshot_account_ids = snapshot_account_ids
        self.description = description
        self.container = container
        self.arch = arch
        self.virt_type = virt_type
        self.root_device_name = root_device_name
        self.volume_type = volume_type
        self.uefi_support = uefi_support
        self.accounts = accounts
        self.groups = groups
        self.tags = tags

    @property
    def _default_snapshot_name(self):
        return os.path.splitext(self.object_name)[0]

    @property
    def object_name(self):
        return os.path.basename(self.image_path).rstrip(".xz")


class DeleteMetadata(object):
    """
    A collection of metadata necessary for deleting a disk image and its
    dependent objects from a cloud provider.

    Args:
        image_id (str): The id of image. Used as a primary identifier.
        image_name (str, optional): The name of the image.
        snapshot_id (str, optional): The id f snaphost.
        snapshot_name (str, optional): The name of the snapshot.
        skip_snapshot (bool, optional): If true, deletion of snapshot is
                                        skipped. Default=False.
    """
    def __init__(self, image_id, image_name=None, snapshot_id=None,
                 snapshot_name=None, skip_snapshot=False):

        self.image_id = image_id
        self.image_name = image_name
        self.snapshot_id = snapshot_id
        self.snapshot_name = snapshot_name
        self.skip_snapshot = skip_snapshot


class BaseService(object):
    """
    Base class for all cloud provider services.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def publish(self, metadata):
        """
        Upload a VM image into a cloud marketplace and make it available for
        all required accounts/groups.

        Args:
            metadata (PublishingMetadata): metadata for the VM image.
        Returns:
            The published object depending on the cloud.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, metadata):
        """
        Delete a VM image from a cloud marketplace and
        all its dependent objects.

        Args:
            metadata (DeleteMetadata): metadata for the VM image.
        """
        raise NotImplementedError
