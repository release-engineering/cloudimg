import unittest

from mock import MagicMock, patch

from cloudimg.common import (
    BaseService,
    ContainerDoesNotExistError,
    ObjectDoesNotExistError,
    PublishingMetadata
)


class TestBaseService(unittest.TestCase):

    def setUp(self):
        self.storage = MagicMock()
        self.compute = MagicMock()
        self.basesvc = BaseService(self.storage, self.compute)

        self.container = MagicMock(name='fakecontainername')
        self.storage.get_container.return_value = self.container

        self.metadata = PublishingMetadata(
            image_path='/some/fake/path/to/image.raw',
            image_name='fakeimagename',
            container=self.container.name
        )

    def test_upload_to_container_create_container(self):
        """
        Test that the container will be created if it doesn't exist.
        """
        self.storage.get_container.side_effect = \
            ContainerDoesNotExistError(None, None, None)
        with patch('cloudimg.common.open'):
            self.basesvc.upload_to_container(self.metadata)
        self.storage.create_container.assert_called_once_with(
            self.container.name)

    @patch('cloudimg.common.open')
    def test_upload_to_container_local_image(self, mock_open):
        """
        Test that upload_object_via_stream is called with the proper args and a
        default name for a local image.
        """
        self.basesvc.upload_to_container(self.metadata)
        stream = mock_open.return_value.__enter__.return_value
        self.storage.upload_object_via_stream.assert_called_once_with(
            stream, self.container, 'image.raw')

    @patch('cloudimg.common.requests')
    def test_upload_to_container_remote_image(self, mock_requests):
        """
        Test that upload_object_via_stream is called with the proper args and a
        default name for a remote image.
        """
        stream = MagicMock()
        mock_requests.get.return_value.iter_content.return_value = stream
        self.metadata.image_path = 'http:///some.fake.url/to/image.raw'
        self.basesvc.upload_to_container(self.metadata)
        self.storage.upload_object_via_stream.assert_called_once_with(
            stream, self.container, 'image.raw')

    def test_get_image(self):
        """
        Test that a get_image returns an image if found.
        """
        image = MagicMock()
        image.name = self.metadata.image_name
        self.compute.list_images.return_value = [image]
        self.assertEqual(self.basesvc.get_image(self.metadata), image)

    def test_get_image_not_found(self):
        """
        Test that get_image returns None when an image is not found.
        """
        image = MagicMock()
        image.name = 'image123'
        self.compute.list_images.return_value = [image]
        self.assertEqual(self.basesvc.get_image(self.metadata), None)

    def test_get_object(self):
        """
        Test that a get_object returns an object if found.
        """
        obj = MagicMock()
        self.storage.get_object.return_value = obj
        self.assertEqual(self.basesvc.get_object(self.metadata), obj)

    def test_get_object_not_found(self):
        """
        Test that get_object returns None when an object is not found.
        """
        self.storage.get_object.side_effect = \
            ObjectDoesNotExistError(None, None, None)
        self.assertEqual(self.basesvc.get_object(self.metadata), None)

    def test_get_snapshot(self):
        """
        Test that a get_snapshot returns a snapshot if found.
        """
        snapshot = MagicMock()
        snapshot.name = self.metadata.snapshot_name
        self.compute.list_snapshots.return_value = [snapshot]
        self.assertEqual(self.basesvc.get_snapshot(self.metadata), snapshot)

    def test_get_snapshot_not_found(self):
        """
        Test that get_snapshot returns None when a snapshot is not found.
        """
        snapshot = MagicMock()
        snapshot.name = 'snapshot123'
        self.compute.list_snapshots.return_value = [snapshot]
        self.assertEqual(self.basesvc.get_snapshot(self.metadata), None)

if __name__ == '__main__':
    unittest.main()
