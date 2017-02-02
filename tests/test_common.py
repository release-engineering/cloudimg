import unittest

from mock import MagicMock, patch

from cloudimg.common import BaseService, ContainerDoesNotExistError


class TestBaseService(unittest.TestCase):

    def setUp(self):
        self.storage = MagicMock()
        self.compute = MagicMock()
        self.basesvc = BaseService(self.storage, self.compute)

        self.image_path = '/some/fake/path/to/image.raw'
        self.image_url = 'http:///some.fake.url/to/image.raw'

        self.container = MagicMock(name='fakecontainername')
        self.storage.get_container.return_value = self.container

    def test_upload_to_container_create_container(self):
        """
        Test that the container will be created if it doesn't exist.
        """
        self.storage.get_container.side_effect = \
            ContainerDoesNotExistError(None, None, None)
        self.basesvc.upload_to_container(self.image_path, self.container.name)
        self.storage.create_container.assert_called_once_with(
            self.container.name)

    def test_upload_to_container_default_name(self):
        """
        Test that upload_object is called with the proper args and a default
        name for the image.
        """
        self.basesvc.upload_to_container(self.image_path, self.container.name)
        self.storage.upload_object.assert_called_once_with(self.image_path,
                                                           self.container,
                                                           'image.raw')

    def test_upload_to_container_name_override(self):
        """
        Test that upload_object is called with the proper args and an override
        name for the image.
        """
        name = 'new-image-name'
        self.basesvc.upload_to_container(self.image_path,
                                         self.container.name,
                                         name=name)
        self.storage.upload_object.assert_called_once_with(self.image_path,
                                                           self.container,
                                                           name)

    @patch('cloudimg.common.requests')
    def test_upload_to_container_default_name_for_url(self, mock_requests):
        """
        Test that upload_object_via_stream is called with the proper args and a
        default name for the image.
        """
        stream = MagicMock()
        mock_requests.get.return_value.iter_content.return_value = stream
        self.basesvc.upload_to_container(self.image_url, self.container.name)
        self.storage.upload_object_via_stream.assert_called_once_with(
            stream, self.container, 'image.raw')

    @patch('cloudimg.common.requests')
    def test_upload_to_container_name_override_for_url(self, mock_requests):
        """
        Test that upload_object_via_stream is called with the proper args and
        override name for the image.
        """
        name = 'new-image-name'
        stream = MagicMock()
        mock_requests.get.return_value.iter_content.return_value = stream
        self.basesvc.upload_to_container(self.image_url,
                                         self.container.name,
                                         name=name)
        self.storage.upload_object_via_stream.assert_called_once_with(
            stream, self.container, name)

if __name__ == '__main__':
    unittest.main()
