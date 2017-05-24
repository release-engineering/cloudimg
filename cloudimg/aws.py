import logging

from libcloud.compute.providers import get_driver as get_compute_driver
from libcloud.storage.providers import get_driver as get_storage_driver
from libcloud.compute.types import Provider as ComputeProvider
from libcloud.storage.types import Provider as StorageProvider

from cloudimg.common import BaseService, PublishingMetadata

log = logging.getLogger(__name__)


class AWSPublishingMetadata(PublishingMetadata):
    """
    A collection of metadata necessary for uploading and publishing a disk
    image to AWS.

    Args:
        ena_support (bool, optional): Enables enhanced network adapters. By
                                      default this option is enabled.
        sriov_net_support (str, optional): Set to 'simple' to enable enhanced
                                           network adapters for Intel 82599 VF
                                           interfaces. By default this option
                                           is enabled.
        billing_products (list, optional): Billing product identifiers
    """

    def __init__(self, *args, **kwargs):
        self.ena_support = kwargs.pop('ena_support', True)
        self.sriov_net_support = kwargs.pop('sriov_net_support', 'simple')
        self.billing_products = kwargs.pop('billing_products', None)

        super(AWSPublishingMetadata, self).__init__(*args, **kwargs)

        assert self.container, 'A container must be defined'


class AWSService(BaseService):
    """
    AWS cloud provider service.

    Args:
        access_id (str): AWS account access ID
        secret_key (str): AWS account secret access key
        region (str, optional): AWS region for compute operations
        import_role (str, optional): AWS IAM role for imports
    """

    def __init__(self, access_id, secret_key, region='us-east-1',
                 import_role=None):
        StorageDriver = self._storage_driver_from_region(region)
        storage = StorageDriver(access_id, secret_key)

        ComputeDriver = get_compute_driver(ComputeProvider.EC2)
        compute = ComputeDriver(access_id, secret_key, region=region)

        self.import_role = import_role

        super(AWSService, self).__init__(storage, compute)

    def _storage_driver_from_region(self, region):
        """
        Searches for a storage driver class matching the region.

        Args:
            region: The name of the AWS region

        Returns:
            A storage driver class if found
        """
        for provider_name in dir(StorageProvider):
            if provider_name.startswith('S3'):
                provider = getattr(StorageProvider, provider_name)
                StorageDriver = get_storage_driver(provider)
                if 'Amazon S3' in StorageDriver.name \
                        and hasattr(StorageDriver, 'region_name') \
                        and StorageDriver.region_name == region:
                    return StorageDriver
        raise ValueError('Cannot find storage driver for region: %s' % region)

    def publish(self, metadata):
        """
        Takes some metadata about a raw disk image, imports it into AWS and
        makes it available to specific accounts/groups.

        This method relies on unique image/metadata names in order to do the
        least amount of work possible. For instance, if the raw disk image has
        already been uploaded to storage but not yet imported as a snapshot,
        the upload will be skipped.

        Args:
            metadata: ``AWSImageMetadata`` object

        Returns:
            A libcloud compute Image object
        """
        log.info('Searching for image: %s', metadata.image_name)
        image = self.get_image(metadata)

        if not image:
            log.info('Image does not exist: %s', metadata.image_name)
            log.info('Searching for snapshot: %s', metadata.snapshot_name)
            snapshot = self.get_snapshot(metadata)

            if not snapshot:
                log.info('Snapshot does not exist: %s', metadata.snapshot_name)
                log.info('Searching for object: %s/%s',
                         metadata.container, metadata.object_name)
                obj = self.get_object(metadata)

                if not obj:
                    log.info('Object does not exist: %s', metadata.object_name)
                    obj = self.upload_to_container(metadata)

                snapshot = self.import_snapshot(obj, metadata)

            image = self.register_image(snapshot, metadata)

        # This is an idempotent operation
        self.share_image(image, metadata)

        return image

    def import_snapshot(self, obj, metadata):
        """
        Imports a disk image as a snapshot.

        Args:
            obj: A libcloud storage object
            metadata: ``AWSImageMetadata`` object

        Returns:
            A libcloud compute VolumeSnapshot object
        """
        source = '%s/%s' % (obj.container.name, obj.name)
        description = 'cloudimg import of %s' % source

        disk_container = [{
            'Description':  description,
            'Format': 'raw',
            'UserBucket': {
                'S3Bucket': obj.container.name,
                'S3Key': obj.name,
            },
        }]

        log.info('Importing snapshot from: %s', source)
        snapshot = self.compute.ex_import_snapshot(
            description=description,
            disk_container=disk_container,
            role_name=self.import_role)

        log.info('Tagging snapshot %s with name: %s',
                 snapshot.id, metadata.snapshot_name)

        # Set the name of the snapshot so we may be able to look it up later
        tags = {'Name': metadata.snapshot_name}
        if not self.compute.ex_create_tags(snapshot, tags):
            log.warning('Failed to set snapshot name for %s', snapshot.id)

        return snapshot

    def register_image(self, snapshot, metadata):
        """
        Registers a snapshot as an image (AMI).

        Args:
            snapshot: A libcloud snapshot object
            metadata: ``AWSImageMetadata`` object

        Returns:
            A libcloud compute Image object
        """

        block_device_mapping = [{
            'DeviceName': metadata.root_device_name,
            'Ebs': {
                'SnapshotId': snapshot.id,
                'VolumeType': metadata.volume_type,
                'DeleteOnTermination': True,
            },
        }]

        log.info('Registering image: %s', metadata.image_name)
        return self.compute.ex_register_image(
            metadata.image_name,
            description=metadata.description,
            architecture=metadata.arch,
            virtualization_type=metadata.virt_type,
            root_device_name=metadata.root_device_name,
            block_device_mapping=block_device_mapping,
            ena_support=metadata.ena_support,
            sriov_net_support=metadata.sriov_net_support,
            billing_products=metadata.billing_products)

    def share_image(self, image, metadata):
        """
        Shares an image with other user accounts or groups.

        Args:
            image: A libcloud image object
            metadata: ``AWSImageMetadata`` object
        """
        # Do nothing if no accounts or groups are specified
        if not metadata.accounts and not metadata.groups:
            return

        perms = {}

        index = 1
        for user_id in metadata.accounts:
            perms['LaunchPermission.Add.%s.UserId' % index] = user_id
            index += 1

        for group in metadata.groups:
            perms['LaunchPermission.Add.%s.Group' % index] = group
            index += 1

        log.info('Sharing %s with accounts: %s',
                 metadata.image_name, metadata.accounts)
        log.info('Sharing %s with groups: %s',
                 metadata.image_name, metadata.groups)
        self.compute.ex_modify_image_attribute(image, perms)
