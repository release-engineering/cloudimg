from tempfile import NamedTemporaryFile
import unittest

from unittest.mock import MagicMock, patch, call, ANY

from cloudimg.ms_azure import (
    AzureDeleteMetadata,
    AzurePublishingMetadata,
    AzureService,
    BlobNotFoundError,
    BlobServiceClient,
    BlobType,
    UploadProgress,
)

from azure.core.exceptions import AzureError


class TestAzurePublishingMetadata(unittest.TestCase):

    def test_container_not_defined(self):
        """
        Test that container must be defined in the metadata.
        """
        self.assertRaises(AssertionError,
                          AzurePublishingMetadata,
                          image_path='/some/fake/path/to/image.vhd',
                          image_name='fakeimagename',
                          tags={'foo': 'bar'})

    def test_tag_not_defined(self):
        """
        Test that tags must be defined in the metadata.
        """
        self.assertRaises(AssertionError,
                          AzurePublishingMetadata,
                          image_path='/some/fake/path/to/image.vhd',
                          image_name='fakeimagename',
                          container='abcdef')


class TestAzureService(unittest.TestCase):

    def setUp(self):
        self.init_service()

    def tearDown(self):
        patch.stopall()

    def init_service(self):
        self.test_account_name = 'foo'
        self.test_account_key = 'bar'
        self.test_connection_str = ('DefaultEndpointsProtocol=https;'
                                    'AccountName=%s;AccountKey=%s;'
                                    'EndpointSuffix=core.windows.net') % (
                                     self.test_account_name,
                                     self.test_account_key,
                                    )
        self.test_blob_svc_client = BlobServiceClient.from_connection_string(
                                            self.test_connection_str,
                                        )
        self.svc = AzureService(
                       blob_service_client=self.test_blob_svc_client,
                       storage_account_name=self.test_account_name,
                       storage_account_key=self.test_account_key,
                   )
        self.md = AzurePublishingMetadata(
            image_path='/some/fake/path/to/image.raw',
            image_name='fakeimagename',
            container='fakecontainername',
            tags={"tag": "tag"},
        )

    def assert_same_attributes(self, obj):
        self.assertEqual(self.svc._account_key, obj._account_key)
        self.assertEqual(self.svc._account_name, obj._account_name)
        self.assertEqual(
            self.svc.blob_service_client,
            obj.blob_service_client,
        )

    def assert_invalid_connection_string(self, conn_str, mock_from_cs):
        self.assertRaises(
                    ValueError,
                    AzureService.from_connection_string,
                    connection_str=conn_str,
        )
        mock_from_cs.assert_not_called()

    @patch('cloudimg.ms_azure.BlobServiceClient.from_connection_string')
    def test_from_connection_string(self, mock_from_cs):
        """
        Test the factory "from_connection_string".
        """
        mock_from_cs.return_value = self.test_blob_svc_client

        obj = AzureService.from_connection_string(self.test_connection_str)

        mock_from_cs.assert_called_once_with(self.test_connection_str)
        self.assert_same_attributes(obj)

    @patch('cloudimg.ms_azure.BlobServiceClient.from_connection_string')
    def test_from_connection_string_invalid(self, mock_from_cs):
        """
        Test the factory "from_connection_string" with an invalid
        connection string.
        """
        self.assert_invalid_connection_string(
            "this_is_an_invalid_connection_string",
            mock_from_cs,
        )

    @patch('cloudimg.ms_azure.BlobServiceClient.from_connection_string')
    def test_from_connection_string_missingkw(self, mock_from_cs):
        """
        Test the factory "from_connection_string" with a connection string
        missing a mandatory keyword.
        """
        missing_kw_cs = self.test_connection_str.replace("AccountName", "Foo")
        self.assert_invalid_connection_string(missing_kw_cs, mock_from_cs)

    @patch('cloudimg.ms_azure.BlobServiceClient.from_connection_string')
    def test_from_connection_string_missingvalue(self, mock_from_cs):
        """
        Test the factory "from_connection_string" with a connection string
        missing a mandatory value.
        """
        missing_vl_cs = self.test_connection_str.replace(
                            self.test_account_name,
                            "",
                        )
        self.assert_invalid_connection_string(missing_vl_cs, mock_from_cs)

    @patch('cloudimg.ms_azure.generate_account_sas')
    @patch('cloudimg.ms_azure.BlobServiceClient')
    def test_from_storage_account(self, mock_blob_svc_cli, mock_gen_acc_sas):
        """
        Test the factory "from_storage_account".
        """
        mock_gen_acc_sas.return_value = "Foo=Bar"
        mock_blob_svc_cli.return_value = self.test_blob_svc_client
        acc_url = "https://%s.blob.core.windows.net" % self.test_account_name

        obj = AzureService.from_storage_account(
                account_name=self.test_account_name,
                account_key=self.test_account_key,
              )

        mock_gen_acc_sas.assert_called_once_with(
            account_name=self.test_account_name,
            account_key=self.test_account_key,
            resource_types=ANY,
            permission=ANY,
            expiry=ANY,
        )
        mock_blob_svc_cli.assert_called_once_with(
            account_url=acc_url,
            credential="Foo=Bar",
        )
        self.assert_same_attributes(obj)

    def test_get_container_by_name_exits(self):
        mock_blob_sc = MagicMock()
        mock_container_client = MagicMock()
        mock_container_client.exists.return_value = True
        self.svc.blob_service_client = mock_blob_sc
        self.svc.blob_service_client.get_container_client.\
            return_value = mock_container_client

        res = self.svc.get_container_by_name(name='testcontainer')

        mock_blob_sc.get_container_client.\
            assert_called_once_with('testcontainer')
        mock_container_client.exists.assert_called_once()
        mock_container_client.create_container.assert_not_called()
        self.assertEqual(res, mock_container_client)

    def test_get_container_by_name_nocreate(self):
        mock_blob_sc = MagicMock()
        mock_container_client = MagicMock()
        mock_container_client.exists.return_value = False
        self.svc.blob_service_client = mock_blob_sc
        self.svc.blob_service_client.get_container_client.\
            return_value = mock_container_client

        res = self.svc.get_container_by_name(
                  name='testcontainer',
                  create=False,
              )

        mock_blob_sc.get_container_client.\
            assert_called_once_with('testcontainer')
        mock_container_client.exists.assert_called_once()
        mock_container_client.create_container.assert_not_called()
        self.assertIsNone(res)

    def test_get_container_by_name_create(self):
        mock_blob_sc = MagicMock()
        mock_container_client = MagicMock()
        mock_container_client.exists.return_value = False
        self.svc.blob_service_client = mock_blob_sc
        self.svc.blob_service_client.get_container_client.\
            return_value = mock_container_client

        res = self.svc.get_container_by_name(
            name='testcontainer',
            create=True,
        )

        mock_blob_sc.get_container_client.\
            assert_called_once_with('testcontainer')
        mock_container_client.exists.assert_called_once()
        mock_container_client.create_container.assert_called_once()
        self.assertEqual(res, mock_container_client)

    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_get_object_by_name_exists(self, mock_get):
        mock_cc = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_get.return_value = mock_cc
        mock_cc.get_blob_client.return_value = mock_blob

        res = self.svc.get_object_by_name(container='foo', name='bar')

        mock_get.assert_called_once_with(name='foo')
        mock_cc.get_blob_client.assert_called_once_with(blob='bar')
        mock_blob.exists.assert_called_once()
        self.assertEqual(res, mock_blob)

    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_get_object_by_name_container_not_found(self, mock_get):
        mock_get.return_value = None

        res = self.svc.get_object_by_name(container='foo', name='bar')

        mock_get.assert_called_once_with(name='foo')
        self.assertIsNone(res)

    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_get_object_by_name_blob_not_found(self, mock_get):
        mock_cc = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_get.return_value = mock_cc
        mock_cc.get_blob_client.return_value = mock_blob

        res = self.svc.get_object_by_name(container='foo', name='bar')

        mock_get.assert_called_once_with(name='foo')
        mock_cc.get_blob_client.assert_called_once_with(blob='bar')
        mock_blob.exists.assert_called_once()
        self.assertIsNone(res)

    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_filter_object_by_tags_exists(self, mock_get):
        """
        Test when the object with tags exists in the first container.
        """
        # Container properties
        mock_cprops = MagicMock()
        mock_cprops.name = 'foo'
        # Blob Service Client
        mock_blob_sc = MagicMock()
        mock_blob_sc.list_containers.return_value = [mock_cprops]
        # Container Client / blobs_list
        mock_cc = MagicMock()
        mock_blob_list = MagicMock()
        mock_blob_list.next.return_value = 'found'
        mock_cc.find_blobs_by_tags.return_value = mock_blob_list
        # Monkeypatching
        mock_get.return_value = mock_cc
        self.svc.blob_service_client = mock_blob_sc

        res = self.svc.filter_object_by_tags(tags=self.md.tags)

        mock_blob_sc.list_containers.assert_called_once()
        mock_get.assert_called_once_with(name=mock_cprops.name)
        mock_cc.find_blobs_by_tags.assert_called_once_with(
            filter_expression="\"tag\"='tag'"
        )
        mock_blob_list.next.assert_called_once()
        self.assertEqual(res, 'found')

    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_filter_object_by_tags_exists_another_container(self, mock_get):
        """
        Test when the object with tags exists in the last container.
        """
        loops = 10
        # Container properties
        mock_cprops = MagicMock()
        mock_cprops.name = 'foo'
        list_containers = [mock_cprops for _ in range(loops)]
        calls = [call(name='foo') for _ in range(loops)]
        # Blob Service Client
        mock_blob_sc = MagicMock()
        mock_blob_sc.list_containers.return_value = list_containers
        # Container Client / blobs_list
        mock_cc = MagicMock()
        mock_blob_list = MagicMock()
        mock_blob_list.next.return_value = 'found'
        mock_cc.find_blobs_by_tags.return_value = mock_blob_list
        # With this the last "container" will be the only one found
        get_containers = [None for _ in range(loops - 1)]
        get_containers.append(mock_cc)
        # Monkeypatching
        mock_get.side_effect = get_containers
        self.svc.blob_service_client = mock_blob_sc

        res = self.svc.filter_object_by_tags(tags=self.md.tags)

        mock_blob_sc.list_containers.assert_called_once()
        mock_get.assert_has_calls(calls=calls)
        assert mock_get.call_count == loops
        mock_cc.find_blobs_by_tags.assert_called_once_with(
            filter_expression="\"tag\"='tag'"
        )
        mock_blob_list.next.assert_called_once()
        self.assertEqual(res, 'found')

    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_filter_object_by_tags_not_found(self, mock_get):
        """
        Test when the object with tags doesn't exists
        """
        loops = 10
        # Container properties
        mock_cprops = MagicMock()
        mock_cprops.name = 'foo'
        list_containers = [mock_cprops for _ in range(loops)]
        calls = [call(name='foo') for _ in range(loops)]
        # Blob Service Client
        mock_blob_sc = MagicMock()
        mock_blob_sc.list_containers.return_value = list_containers
        # Monkeypatching
        # NOTE: ContainerClient == None
        mock_get.return_value = None
        self.svc.blob_service_client = mock_blob_sc

        res = self.svc.filter_object_by_tags(tags=self.md.tags)

        mock_blob_sc.list_containers.assert_called_once()
        mock_get.assert_has_calls(calls=calls)
        assert mock_get.call_count == loops
        self.assertIsNone(res)

    def test_are_tags_present_true(self):
        mock_cc = MagicMock()
        mock_blob_list = MagicMock()
        mock_cc.find_blobs_by_tags.return_value = mock_blob_list

        res = self.svc.are_tags_present(
                  container_client=mock_cc,
                  tags=self.md.tags
              )

        mock_cc.find_blobs_by_tags.assert_called_once_with(
            filter_expression="\"tag\"='tag'"
        )
        mock_blob_list.next.assert_called_once()
        self.assertTrue(res)

    def test_are_tags_present_false(self):
        mock_cc = MagicMock()
        mock_blob_list = MagicMock()
        mock_blob_list.next.side_effect = StopIteration
        mock_cc.find_blobs_by_tags.return_value = mock_blob_list

        res = self.svc.are_tags_present(
                  container_client=mock_cc,
                  tags=self.md.tags
              )

        mock_cc.find_blobs_by_tags.assert_called_once_with(
            filter_expression="\"tag\"='tag'"
        )
        mock_blob_list.next.assert_called_once()
        self.assertFalse(res)

    def test_upload_callback(self):
        current = 1024
        total = 4096
        response = MagicMock()
        callback = {
            'upload_stream_current': current,
            'data_stream_total': total
        }
        response.context = callback
        self.svc.UPLOAD_LOG_INTERVAL_SECONDS = 0

        res = self.svc.upload_callback(response)

        self.assertIsInstance(res, UploadProgress)
        self.assertEqual(res.current, current)
        self.assertEqual(res.total, total)
        self.assertEqual(res.percentage, current / total * 100)

    @patch('cloudimg.ms_azure.AzureService.get_object_by_name')
    @patch('cloudimg.ms_azure.AzureService.filter_object_by_tags')
    @patch('cloudimg.ms_azure.AzureService.are_tags_present')
    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_upload_to_container_tag_already_present(self,
                                                     mock_get,
                                                     mock_are_tags,
                                                     mock_filter_obj,
                                                     mock_get_obj_name,
                                                     ):
        mock_blob = MagicMock()
        mock_cc = MagicMock()
        mock_cc.get_blob_client = mock_blob
        mock_get.return_value = mock_cc
        mock_are_tags.return_value = True
        mock_filter_obj.return_value = MagicMock()
        mock_filter_obj.return_value.container_name = self.md.container
        mock_filter_obj.return_value.name = self.md.object_name
        upload_progress = UploadProgress(current=0, total=0)

        res = self.svc.upload_to_container(
            image_path=self.md.image_path,
            container_name=self.md.container,
            object_name=self.md.object_name,
            tags=self.md.tags,
        )

        mock_filter_obj.assert_called_once_with(self.md.tags)
        mock_get_obj_name.assert_called_once_with(
            container=self.md.container,
            name=self.md.object_name,
        )
        mock_get.assert_called_once_with(name=self.md.container, create=True)
        mock_are_tags.assert_called_once_with(mock_cc, self.md.tags)
        mock_cc.get_blob_client.assert_not_called()
        mock_blob.exists.assert_not_called()
        self.assertEqual(self.svc._upload_progress, upload_progress)
        assert res

    @patch('cloudimg.ms_azure.AzureService.get_object_by_name')
    @patch('cloudimg.ms_azure.AzureService.are_tags_present')
    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_upload_to_container_blob_already_present(self,
                                                      mock_get,
                                                      mock_are_tags,
                                                      mock_get_obj_name):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_cc = MagicMock()
        mock_cc.get_blob_client.return_value = mock_blob
        mock_get.return_value = mock_cc
        mock_are_tags.return_value = False
        upload_progress = UploadProgress(current=0, total=0)

        res = self.svc.upload_to_container(
            image_path=self.md.image_path,
            container_name=self.md.container,
            object_name=self.md.object_name,
            tags=self.md.tags,
        )

        mock_get_obj_name.assert_called_once_with(
            container=self.md.container,
            name=self.md.object_name,
        )
        mock_get.assert_called_once_with(name=self.md.container, create=True)
        mock_are_tags.assert_called_once_with(mock_cc, self.md.tags)
        mock_cc.get_blob_client.assert_called_once_with(
            blob=self.md.object_name
        )
        mock_blob.exists.assert_called_once()
        self.assertEqual(self.svc._upload_progress, upload_progress)
        assert res

    @patch('cloudimg.ms_azure.AzureService.upload_callback')
    @patch('cloudimg.ms_azure.AzureService.are_tags_present')
    @patch('cloudimg.ms_azure.AzureService.get_container_by_name')
    def test_upload_to_container_blob_from_storage(self,
                                                   mock_get,
                                                   mock_are_tags,
                                                   mock_callback):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_cc = MagicMock()
        mock_cc.get_blob_client.return_value = mock_blob
        mock_get.return_value = mock_cc
        mock_are_tags.return_value = False
        mock_blob.upload_blob.side_effect = mock_callback

        with NamedTemporaryFile() as tmpfile:
            # Write some testing data
            tmpfile.seek(1020)
            tmpfile.write(b"1234")
            tmpfile.flush()

            # Test the upload
            res = self.svc.upload_to_container(
                    image_path=tmpfile.name,
                    container_name=self.md.container,
                    object_name=self.md.object_name,
                    tags=self.md.tags
                )

        mock_get.assert_called_once_with(name=self.md.container, create=True)
        mock_are_tags.assert_called_once_with(mock_cc, self.md.tags)
        mock_cc.get_blob_client.assert_called_once_with(
            blob=self.md.object_name
        )
        mock_blob.exists.assert_called_once()
        mock_blob.upload_blob.assert_called_once_with(
            data=ANY,
            blob_type=BlobType.PAGEBLOB,
            length=1024,
            tags=self.md.tags,
            max_concurrency=self.svc.UPLOAD_MAX_CONCURRENCY,
            raw_response_hook=mock_callback,
        )
        mock_callback.assert_called_once()
        self.assertEqual(res, mock_blob)

    @patch("cloudimg.ms_azure.AzureService.are_tags_present")
    @patch("cloudimg.ms_azure.AzureService.get_container_by_name")
    def test_copy_to_container_failed(
        self,
        mock_get_container_by_name,
        mock_are_tags_present,
    ):
        mock_blob_client = MagicMock()
        mock_blob_client.exists.return_value = False
        mock_container_client = MagicMock()
        mock_container_client.get_blob_client.return_value = mock_blob_client
        mock_get_container_by_name.return_value = mock_container_client
        mock_are_tags_present.return_value = False

        mock_blob_client.get_blob_properties.side_effect = [
            {"copy": {"status": "Pending", "progress": "0/3"}},
            {"copy": {"status": "Pending", "progress": "1/3"}},
            {"copy": {"status": "Pending", "progress": "2/3"}},
        ]
        with self.assertRaises(AzureError) as context:
            with patch("time.sleep", return_value=0) as mock_sleep:
                self.svc.upload_to_container(
                    image_path="https://example.com/rhcos-azure.x86_64.vhd",
                    container_name=self.md.container,
                    object_name=self.md.object_name,
                    tags=self.md.tags,
                )
        self.assertTrue("RetryError" in str(context.exception))
        mock_blob_client.exists.assert_called_once()
        assert mock_sleep.call_count == 59
        mock_blob_client.start_copy_from_url.assert_called_once_with(
            source_url="https://example.com/rhcos-azure.x86_64.vhd",
            metadata={},
            incremental_copy=False)

        mock_are_tags_present.assert_called_once_with(
            mock_container_client, self.md.tags
            )

    @patch("cloudimg.ms_azure.AzureService.are_tags_present")
    @patch("cloudimg.ms_azure.AzureService.get_container_by_name")
    def test_copy_to_container_from_url(
        self,
        mock_get_container_by_name,
        mock_are_tags_present,
    ):
        mock_blob_client = MagicMock()
        mock_blob_client.exists.return_value = False
        mock_container_client = MagicMock()
        mock_container_client.get_blob_client.return_value = mock_blob_client
        mock_get_container_by_name.return_value = mock_container_client
        mock_are_tags_present.return_value = False

        mock_blob_client.get_blob_properties.side_effect = [
            {"copy": {"status": "Pending", "progress": "0/2"}},
            {"copy": {"status": "Pending", "progress": "1/2"}},
            {"copy": {"status": "success", "progress": "2/2"}},
        ]
        with patch("time.sleep", return_value=0) as mock_sleep:
            res = self.svc.upload_to_container(
                image_path="https://example.com/rhcos-azure.x86_64.vhd",
                container_name=self.md.container,
                object_name=self.md.object_name,
                tags=self.md.tags,
            )

        mock_blob_client.exists.assert_called_once()
        assert mock_sleep.call_count == 2
        mock_blob_client.start_copy_from_url.assert_called_once_with(
            source_url="https://example.com/rhcos-azure.x86_64.vhd",
            metadata={},
            incremental_copy=False)

        mock_are_tags_present.assert_called_once_with(
            mock_container_client, self.md.tags
            )
        self.assertEqual(res, mock_blob_client)

    @patch('cloudimg.ms_azure.AzureService.upload_to_container')
    @patch('cloudimg.ms_azure.AzureService.filter_object_by_tags')
    @patch('cloudimg.ms_azure.AzureService.get_object_by_name')
    def test_publish(self, mock_get_obj, mock_filter_by_tags, mock_upload):
        mock_blob = MagicMock()
        mock_blob.get_blob_properties.return_value = "props"
        mock_get_obj.return_value = None
        mock_filter_by_tags.return_value = None
        mock_upload.return_value = mock_blob

        res = self.svc.publish(self.md)

        mock_get_obj.assert_called_once_with(
            container=self.md.container,
            name=self.md.object_name,
        )
        mock_filter_by_tags.assert_called_once_with(self.md.tags)
        mock_upload.assert_called_once_with(
            image_path=self.md.image_path,
            container_name=self.md.container,
            object_name=self.md.object_name,
            tags=self.md.tags,
        )
        self.assertEqual(res, "props")

    @patch('cloudimg.ms_azure.AzureService.upload_to_container')
    @patch('cloudimg.ms_azure.AzureService.filter_object_by_tags')
    @patch('cloudimg.ms_azure.AzureService.get_object_by_name')
    def test_publish_tag_found(self, mock_get_obj, mock_filter_by_tags,
                               mock_upload):
        mock_blob = MagicMock()
        mock_blob.get_blob_properties.return_value = "props"
        mock_filtered = MagicMock()
        mock_filtered.container_name = self.md.container
        mock_filtered.name = self.md.object_name
        mock_get_obj.side_effect = [None, mock_blob]
        mock_filter_by_tags.return_value = mock_filtered
        calls = [
            call(container=self.md.container, name=self.md.object_name),
            call(container=self.md.container, name=self.md.object_name),
        ]

        res = self.svc.publish(self.md)

        mock_get_obj.assert_has_calls(calls)
        mock_filter_by_tags.assert_called_once_with(self.md.tags)
        mock_upload.assert_not_called()
        self.assertEqual(res, "props")

    @patch('cloudimg.ms_azure.AzureService.upload_to_container')
    @patch('cloudimg.ms_azure.AzureService.filter_object_by_tags')
    @patch('cloudimg.ms_azure.AzureService.get_object_by_name')
    def test_publish_blob_found(self, mock_get_obj, mock_filter_by_tags,
                                mock_upload):
        mock_blob = MagicMock()
        mock_blob.get_blob_properties.return_value = "props"
        mock_filtered = MagicMock()
        mock_filtered.container_name = self.md.container
        mock_filtered.name = self.md.object_name
        mock_get_obj.return_value = mock_blob
        mock_filter_by_tags.return_value = mock_filtered

        res = self.svc.publish(self.md)

        mock_get_obj.assert_called_once_with(
            container=self.md.container,
            name=self.md.object_name
        )
        mock_filter_by_tags.assert_not_called()
        mock_upload.assert_not_called()
        self.assertEqual(res, "props")

    @patch('cloudimg.ms_azure.generate_blob_sas')
    def test_get_blob_sas_uri(self, mock_gen_blob_sas):
        mock_blobprops = MagicMock()
        mock_blobprops.container = 'foo'
        mock_blobprops.name = 'bar'
        mock_gen_blob_sas.return_value = "Foo=Bar"
        expected_res = "https://%s.blob.core.windows.net/foo/bar?Foo=Bar" % (
                self.svc._account_name,
            )

        sas = self.svc.get_blob_sas_uri(mock_blobprops)

        mock_gen_blob_sas.assert_called_once_with(
            account_name=self.test_account_name,
            account_key=self.test_account_key,
            container_name='foo',
            blob_name='bar',
            permission=ANY,
            expiry=ANY,
        )
        self.assertEqual(sas, expected_res)

    @patch('cloudimg.ms_azure.AzureService.get_object_by_name')
    def test_tag_image(self, mock_get_obj):
        mock_blobprops = MagicMock()
        mock_blobprops.container = 'foo'
        mock_blobprops.name = 'bar'
        mock_blob = MagicMock()
        mock_get_obj.return_value = mock_blob
        mock_blob.set_blob_tags.return_value = True
        tags = {"tag": "tag"}

        res = self.svc.tag_image(blob_props=mock_blobprops, tags=tags)

        mock_get_obj.assert_called_once_with(
            container='foo',
            name='bar',
        )

        mock_blob.set_blob_tags.assert_called_once_with(tags)
        self.assertTrue(res)

    @patch('cloudimg.ms_azure.AzureService.get_object_by_name')
    def test_tag_image_not_found(self, mock_get_obj):
        mock_blobprops = MagicMock()
        mock_blobprops.container = 'foo'
        mock_blobprops.name = 'bar'
        mock_get_obj.return_value = None
        tags = {"tag": "tag"}

        self.assertRaises(BlobNotFoundError,
                          self.svc.tag_image,
                          blob_props=mock_blobprops,
                          tags=tags)

        mock_get_obj.assert_called_once_with(
            container='foo',
            name='bar',
        )

    @patch("cloudimg.ms_azure.AzureService.get_object_by_name")
    def test_delete_image_exists(self, mock_get_obj):
        image_name = "fake_image_name"
        container_name = "fake_container_name"
        mock_blob = MagicMock()
        mock_blob.container_name = container_name
        mock_blob.blob_name = image_name
        mock_get_obj.return_value = mock_blob

        delete_meta = AzureDeleteMetadata(
            image_id=image_name,
            container=container_name,
        )

        # run delete
        deleted_img_name, deleted_img_path = self.svc.delete(delete_meta)

        # Ensure the metadata's `image_name` and `image_id` are the same
        assert delete_meta.image_id == delete_meta.image_name

        # Ensure the calls were properly made
        mock_get_obj.assert_called_once_with(
            container=container_name,
            name=image_name,
        )
        mock_blob.delete_blob.assert_called_once_with(
            delete_snapshots="include",
        )

        # Ensure the return is the expected ones
        assert deleted_img_name == image_name
        assert deleted_img_path == "%s/%s" % (container_name, image_name)

    @patch("cloudimg.ms_azure.AzureService.get_object_by_name")
    def test_delete_image_missing(self, mock_get_obj):
        image_name = "fake_image_name"
        container_name = "fake_container_name"
        mock_get_obj.return_value = None

        delete_meta = AzureDeleteMetadata(
            image_id=image_name,
            container=container_name,
        )

        # run delete
        deleted_img_name, deleted_img_path = self.svc.delete(delete_meta)

        # Ensure the metadata's `image_name` and `image_id` are the same
        assert delete_meta.image_id == delete_meta.image_name

        # Ensure the calls were properly made
        mock_get_obj.assert_called_once_with(
            container=container_name,
            name=image_name,
        )

        # Ensure the return is the expected ones
        assert not deleted_img_name
        assert not deleted_img_path


if __name__ == '__main__':
    unittest.main()
