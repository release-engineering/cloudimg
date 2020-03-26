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
        accounts (list, optional): Accounts which will have permission to use
                                   the image
        groups (list, optional): Groups which will have permission to use the
                                 image
    """

    def __init__(self, image_path, image_name, description=None,
                 container=None, arch=None, virt_type=None,
                 root_device_name=None, volume_type=None,
                 accounts=[], groups=[], snapshot_name=None):
        self.image_path = image_path
        self.image_name = image_name
        self.snapshot_name = snapshot_name or self._default_snapshot_name
        self.description = description
        self.container = container
        self.arch = arch
        self.virt_type = virt_type
        self.root_device_name = root_device_name
        self.volume_type = volume_type
        self.accounts = accounts
        self.groups = groups

    @property
    def _default_snapshot_name(self):
        return os.path.splitext(self.object_name)[0]

    @property
    def object_name(self):
        return os.path.basename(self.image_path)


class BaseService(object):
    """
    Base class for all cloud provider services.
    """
    pass
