import logging

from libcloud.compute.providers import get_driver as get_compute_driver
from libcloud.storage.providers import get_driver as get_storage_driver
from libcloud.compute.types import Provider as ComputeProvider
from libcloud.storage.types import Provider as StorageProvider

from cloudimg import config
from cloudimg.common import BaseService

log = logging.getLogger(__name__)


class AWSService(BaseService):
    """
    AWS cloud provider service.
    """

    def __init__(self):
        StorageDriver = get_storage_driver(StorageProvider.S3)
        storage = StorageDriver(config.AWS_ACCESS_ID,
                                secret=config.AWS_SECRET_KEY)

        ComputeDriver = get_compute_driver(ComputeProvider.EC2)
        compute = ComputeDriver(config.AWS_ACCESS_ID,
                                config.AWS_SECRET_KEY,
                                config.AWS_DEFAULT_REGION)

        super(AWSService, self).__init__(storage, compute)

    def upload(self, image, name=None):
        return self.upload_to_container(image,
                                        config.AWS_STORAGE_CONTAINER,
                                        name=name)
