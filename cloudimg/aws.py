import logging
from uuid import uuid1

from libcloud.compute.providers import get_driver as get_compute_driver
from libcloud.storage.providers import get_driver as get_storage_driver
from libcloud.compute.types import Provider as ComputeProvider
from libcloud.storage.types import Provider as StorageProvider

from cloudimg.common import BaseService

log = logging.getLogger(__name__)


class AWSService(BaseService):
    """
    AWS cloud provider service.

    Args:
        access_id (str): AWS account access ID
        secret_key (str): AWS account secret access key
        region (str, optional): AWS region for compute operations
    """

    # Auto generated storage container if none is provided on upload
    STORAGE_CONTAINER = 'cloudimg-' + str(uuid1())

    def __init__(self, access_id, secret_key, region='us-east-1'):
        StorageDriver = get_storage_driver(StorageProvider.S3)
        storage = StorageDriver(access_id, secret_key)

        ComputeDriver = get_compute_driver(ComputeProvider.EC2)
        compute = ComputeDriver(access_id, secret_key, region)

        super(AWSService, self).__init__(storage, compute)

    def upload(self, image, name=None, container=STORAGE_CONTAINER):
        """
        Refer to ``cloudimg.common.BaseService.upload`` for more info.

        Args:
            container (str, optional): The storage container where the image
                                       will be stored. Auto generated if not
                                       specified.
        """
        return self.upload_to_container(image, container, name=name)
