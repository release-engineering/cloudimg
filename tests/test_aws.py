import unittest

from mock import MagicMock, patch
from libcloud.storage.drivers import s3

from cloudimg.aws import AWSService, AWSPublishingMetadata


class TestAWSPublishingMetadata(unittest.TestCase):

    def test_container_not_defined(self):
        """
        Test that container must be defined in the metadata.
        """
        self.assertRaises(AssertionError,
                          AWSPublishingMetadata,
                          image_path='/some/fake/path/to/image.raw',
                          image_name='fakeimagename')


class TestAWSService(unittest.TestCase):

    REGIONS_TO_S3_DRIVERS = {
        'us-east-1': s3.S3StorageDriver,
        'us-east-2': s3.S3USEast2StorageDriver,
        'us-west-1': s3.S3USWestStorageDriver,
        'us-west-2': s3.S3USWestOregonStorageDriver,
        'us-gov-west-1': s3.S3USGovWestStorageDriver,
        'ca-central-1': s3.S3CACentralStorageDriver,
        'sa-east-1': s3.S3SAEastStorageDriver,
        'eu-west-1': s3.S3EUWestStorageDriver,
        'eu-west-2': s3.S3EUWest2StorageDriver,
        'eu-central-1': s3.S3EUCentralStorageDriver,
        'ap-south-1': s3.S3APSouthStorageDriver,
        'ap-northeast-1': s3.S3APNE1StorageDriver,
        'ap-northeast-2': s3.S3APNE2StorageDriver,
        'ap-southeast-1': s3.S3APSEStorageDriver,
        'ap-southeast-2': s3.S3APSE2StorageDriver,
        'cn-north-1': s3.S3CNNorthStorageDriver,
    }

    def setUp(self):
        self.svc = AWSService('fakeaccessid', 'fakesecretkey')
        self.svc.storage = MagicMock()
        self.svc.compute = MagicMock()

        self.metadata = AWSPublishingMetadata(
            image_path='/some/fake/path/to/image.raw',
            image_name='fakeimagename',
            container='fakecontainername'
        )

    def test_find_storage_driver_by_region(self):
        """
        Test that the storage driver can be acquired given the region name.
        """
        for region, driver in self.REGIONS_TO_S3_DRIVERS.items():
            found = self.svc._storage_driver_from_region(region)
            self.assertEqual(driver, found)

    def test_cannot_find_storage_driver_by_region(self):
        """
        Test that the storage driver cannot be acquired given a bad region
        name.
        """
        self.assertRaises(ValueError, self.svc._storage_driver_from_region,
                          'us-east-never-to-be-found')

    def test_share_image(self):
        """
        Test that share_image modifies the image attributes with the correct
        launch permissions.
        """
        self.metadata.accounts = ['account1', 'account2']
        self.metadata.groups = ['group1', 'group2']

        image = MagicMock()

        perms = {
            'LaunchPermission.Add.1.UserId': 'account1',
            'LaunchPermission.Add.2.UserId': 'account2',
            'LaunchPermission.Add.3.Group': 'group1',
            'LaunchPermission.Add.4.Group': 'group2',
        }

        self.svc.share_image(image, self.metadata)
        self.svc.compute.ex_modify_image_attribute\
            .assert_called_once_with(image, perms)

    def test_share_image_no_op(self):
        """
        Test that share_image does nothing if no accounts or groups are
        specified.
        """
        self.metadata.accounts = []
        self.metadata.groups = []

        image = MagicMock()

        self.svc.share_image(image, self.metadata)
        self.svc.compute.ex_modify_image_attribute.assert_not_called()

    @patch('cloudimg.aws.AWSService.upload_to_container')
    @patch('cloudimg.aws.AWSService.import_snapshot')
    @patch('cloudimg.aws.AWSService.register_image')
    @patch('cloudimg.aws.AWSService.share_image')
    @patch('cloudimg.aws.AWSService.get_image')
    @patch('cloudimg.aws.AWSService.get_snapshot')
    @patch('cloudimg.aws.AWSService.get_object')
    def test_publish_image_found(self, get_object, get_snapshot, get_image,
                                 share_image, register_image, import_snapshot,
                                 upload_to_container):
        image = MagicMock()
        get_image.return_value = image
        published = self.svc.publish(self.metadata)
        self.assertEqual(image, published)

        share_image.assert_called_once_with(image, self.metadata)

        get_snapshot.assert_not_called()
        get_object.assert_not_called()
        register_image.assert_not_called()
        import_snapshot.assert_not_called()
        upload_to_container.assert_not_called()

    @patch('cloudimg.aws.AWSService.upload_to_container')
    @patch('cloudimg.aws.AWSService.import_snapshot')
    @patch('cloudimg.aws.AWSService.register_image')
    @patch('cloudimg.aws.AWSService.share_image')
    @patch('cloudimg.aws.AWSService.get_image')
    @patch('cloudimg.aws.AWSService.get_snapshot')
    @patch('cloudimg.aws.AWSService.get_object')
    def test_publish_snapshot_found(self, get_object, get_snapshot, get_image,
                                    share_image, register_image,
                                    import_snapshot, upload_to_container):
        snapshot = MagicMock()
        get_image.return_value = None
        get_snapshot.return_value = snapshot
        published = self.svc.publish(self.metadata)

        register_image.assert_called_once_with(snapshot, self.metadata)
        share_image.assert_called_once_with(published, self.metadata)

        get_object.assert_not_called()
        import_snapshot.assert_not_called()
        upload_to_container.assert_not_called()

    @patch('cloudimg.aws.AWSService.upload_to_container')
    @patch('cloudimg.aws.AWSService.import_snapshot')
    @patch('cloudimg.aws.AWSService.register_image')
    @patch('cloudimg.aws.AWSService.share_image')
    @patch('cloudimg.aws.AWSService.get_image')
    @patch('cloudimg.aws.AWSService.get_snapshot')
    @patch('cloudimg.aws.AWSService.get_object')
    def test_publish_object_found(self, get_object, get_snapshot, get_image,
                                  share_image, register_image, import_snapshot,
                                  upload_to_container):
        obj = MagicMock()
        get_image.return_value = None
        get_snapshot.return_value = None
        get_object.return_value = obj
        published = self.svc.publish(self.metadata)

        register_image.assert_called_once()
        share_image.assert_called_once_with(published, self.metadata)
        import_snapshot.assert_called_once_with(obj, self.metadata)

        upload_to_container.assert_not_called()

    @patch('cloudimg.aws.AWSService.upload_to_container')
    @patch('cloudimg.aws.AWSService.import_snapshot')
    @patch('cloudimg.aws.AWSService.register_image')
    @patch('cloudimg.aws.AWSService.share_image')
    @patch('cloudimg.aws.AWSService.get_image')
    @patch('cloudimg.aws.AWSService.get_snapshot')
    @patch('cloudimg.aws.AWSService.get_object')
    def test_publish(self, get_object, get_snapshot, get_image, share_image,
                     register_image, import_snapshot, upload_to_container):
        get_image.return_value = None
        get_snapshot.return_value = None
        get_object.return_value = None
        published = self.svc.publish(self.metadata)

        share_image.assert_called_once_with(published, self.metadata)
        register_image.assert_called_once()
        import_snapshot.assert_called_once()
        upload_to_container.assert_called_once_with(self.metadata)

if __name__ == '__main__':
    unittest.main()
