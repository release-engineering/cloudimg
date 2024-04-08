import enum
import functools
import logging
import os
import threading
import time
import lzma

from boto3.session import Session
from botocore.exceptions import ClientError
import requests

from cloudimg.common import BaseService, PublishingMetadata, DeleteMetadata

log = logging.getLogger(__name__)


class SnapshotError(Exception):
    """
    Raised when a snapshot related error occurs.
    """
    pass


class SnapshotTimeout(Exception):
    """
    Raised when a snapshot related timeout occurs.
    """
    pass


class AWSBootMode(enum.Enum):
    """The boot mode options supported by AWS when registering an AMI."""

    uefi = "uefi"
    """Support UEFI only."""

    legacy = "legacy-bios"
    """Support BIOS only."""

    hybrid = "uefi-preferred"
    """Support UEFI and BIOS giving the preference to UEFI."""

    not_set = None
    """Use the default boot mode from AWS."""


def log_request_id(func):
    """
    The AWS request ID is useful when troubleshooting errors. When a
    botocore.exceptions.ClientError error is raised, attempt to log the
    request ID associated with the error.

    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html#catching-exceptions-when-using-a-low-level-client
    """
    @functools.wraps(func)
    def log_request_id_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as exc:
            if exc.response.get("ResponseMetadata") and \
                    exc.response['ResponseMetadata'].get('RequestId'):
                log.exception(
                    "An error occurred in AWS (AWS RequestId: %s)",
                    exc.response['ResponseMetadata']['RequestId']
                )
            raise
    return log_request_id_wrapper


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

        boot_mode (str, optional): The boot mode for booting up the AMI on EC2.
    """

    def __init__(self, *args, **kwargs):
        self.ena_support = kwargs.pop('ena_support', True)
        self.sriov_net_support = kwargs.pop('sriov_net_support', 'simple')
        self.billing_products = kwargs.pop('billing_products', None)
        bmode_str = kwargs.pop('boot_mode', None) or "not_set"
        self.boot_mode = AWSBootMode[bmode_str]

        super(AWSPublishingMetadata, self).__init__(*args, **kwargs)

        # Set the UEFI mode when the boolean is used instead of Enum.
        if (
            self.boot_mode == AWSBootMode.not_set and
            self.uefi_support is not None
        ):
            self.boot_mode = AWSBootMode.hybrid if self.uefi_support \
                             else AWSBootMode.legacy

        assert self.container, 'A container must be defined'


class AWSDeleteMetadata(DeleteMetadata):
    pass


class UploadProgress(object):
    """
    Progress callback for file uploads. Can report determinate or indeterminate
    progress. Expected to be called as a function.

    Ex:
        callback = UploadProgress('mybucket', 'myfile', filepath='/some/file')
        callback(1024)
        callback(1024)
        callback(1024)
        ...

    Args:
        container_name (str): Name of the container being uploaded to
        object_name (str): Name of the destination object
        filepath (str, optional): Path to the file being uploaded. None for
                                  indeterminate progress.
        interval (int, optional): Seconds between logging updates
    """

    def __init__(self, container_name, object_name, filepath=None,
                 interval=15):
        self.container_name = container_name
        self.object_name = object_name

        if filepath is not None:
            self._size = os.path.getsize(filepath)
        else:
            self._size = None

        self._interval = interval

        # Total bytes uploaded
        self._seen = 0

        # Time of last log message
        self._last_log = 0

        # Lock for multithreaded uploads
        self._lock = threading.Lock()

    @property
    def determinate(self):
        """
        Uploads are determinate if they have a total expected upload size.
        """
        return self._size is not None

    @property
    def done(self):
        """
        Only supported for determinate uploads.
        """
        assert self.determinate, 'done unsupported for indeterminate uploads'
        return self._size == self._seen

    def __call__(self, bytes_):
        with self._lock:
            self._seen += bytes_

            # Determine if the time lapse since the last log is greater than
            # the interval.
            now = time.time()
            overdue = now - self._last_log >= self._interval

            # Log determinate when overdue or at 100%
            if self.determinate and (self.done or overdue):
                percentage = (float(self._seen) / self._size) * 100
                log.info('Bytes uploaded (%s/%s): %s/%s (%.2f%%)',
                         self.container_name, self.object_name, self._seen,
                         self._size, percentage)
                self._last_log = now

            # Log indeterminate only when overdue
            elif not self.determinate and overdue:
                log.info('Bytes uploaded (%s/%s): %s', self.container_name,
                         self.object_name, self._seen)
                self._last_log = now


class AWSService(BaseService):
    """
    AWS cloud provider service.

    Args:
        access_id (str): AWS account access ID
        secret_key (str): AWS account secret access key
        region (str, optional): AWS region for compute operations
        import_role (str, optional): AWS IAM role for imports
    """

    # Upload chunk size
    CHUNK_SIZE = 4 * 1024 * 1024  # 4MB

    def __init__(self, access_id, secret_key, region='us-east-1',
                 import_role=None):
        self.session = Session(
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key,
            region_name=region
        )

        self.ec2 = self.session.resource('ec2')
        self.s3 = self.session.resource('s3')

        self.import_role = import_role

        super(AWSService, self).__init__()

    def get_image_by_filters(self, filters):
        """
        Finds an image with the given filters.

        Args:
            filters (list): List with the filters (dict) to apply.

        Returns:
            An EC2 Image if found; None otherwise
        """
        rsp = self.ec2.meta.client.describe_images(
            Owners=['self'],
            Filters=filters,
        )

        images = rsp['Images']

        if not images:
            return None

        return self.ec2.Image(images[0]['ImageId'])

    def get_image_from_ami_catalog(self, image_id):
        """
        Finds Image in AMI Catalog with given image id.
        The search includes AMIs owned by self, AWS marketplace AMIs,
        and Community AMIs.

        Args:
            id(str): Image ID of AMI.

        Returns:
            An EC2 Image if found; None other wise.
        """

        filters = [{
            "Name": "image-id",
            "Values": [image_id],
        }]
        rsp = self.ec2.meta.client.describe_images(Filters=filters)

        images = rsp["Images"]

        if not images:
            return None

        return self.ec2.Image(images[0]["ImageId"])

    def get_image_by_name(self, name):
        """
        Finds an image with a given name.

        Args:
            name (str): The name of the image

        Returns:
            An EC2 Image if found; None otherwise
        """
        if not name:
            return None

        filters = [
            {
                'Name': 'name',
                'Values': [name],
            }
        ]

        return self.get_image_by_filters(filters)

    def get_image_by_tags(self, tags):
        """
        Finds an image with the given tags.

        Args:
            tags (dict): The tags to filter the image

        Returns:
            An EC2 Image if found; None otherwise
        """
        if not tags:
            return None

        filters = [
            {
                'Name': 'tag:%s' % k,
                'Values': [v],
            }
            for k, v in tags.items()
        ]

        return self.get_image_by_filters(filters)

    def get_image_by_id(self, image_id):
        """
        Finds an image by image id.

        Args:
            id (str): The id of the image

        Returns:
            An EC2 Image if found; None otherwise
        """
        if not image_id:
            return None

        filters = [
            {
                'Name': 'image-id',
                'Values': [image_id],
            }
        ]

        return self.get_image_by_filters(filters)

    def get_snapshot_by_name(self, name):
        """
        Finds a snapshot with a given name.

        Args:
            name (str): The name of the snapshot

        Returns:
            An EC2 Snapshot if found; None otherwise
        """
        if not name:
            return None

        rsp = self.ec2.meta.client.describe_snapshots(
            OwnerIds=['self'],
            Filters=[{
                'Name': 'tag:Name',
                'Values': [name],
            }],
        )

        snapshots = rsp['Snapshots']

        if not snapshots:
            return None

        return self.ec2.Snapshot(snapshots[0]['SnapshotId'])

    def get_snapshot_by_id(self, snapshot_id):
        """
        Finds a snapshot by snapshot id.

        Args:
            snapshot_id (str): The id of the snapshot

        Returns:
            An EC2 Snapshot if found; None otherwise
        """
        if not snapshot_id:
            return None

        rsp = self.ec2.meta.client.describe_snapshots(
            OwnerIds=['self'],
            Filters=[{
                'Name': 'snapshot-id',
                'Values': [snapshot_id],
            }],
        )

        snapshots = rsp['Snapshots']

        if not snapshots:
            return None

        return self.ec2.Snapshot(snapshots[0]['SnapshotId'])

    def get_object_by_name(self, container_name, name):
        """
        Finds an object with a given name.

        Args:
            container_name (str): The name of the container to search in
            name (str): The name of the object

        Returns:
            An S3 Object if found; None otherwise
        """
        obj = self.s3.Object(container_name, name)
        try:
            obj.load()
        except ClientError as error:
            if int(error.response['Error']['Code']) == 404:
                return None
            raise

        return obj

    def get_container_by_name(self, name):
        """
        Finds a container with a given name.

        Args:
            name (str): The name of the container

        Returns:
            An S3 Bucket if found; None otherwise
        """
        try:
            # Calling load() on a bucket does not raise errors. Instead, the
            # recommended way from the docs is to perform a HEAD operation.
            self.s3.meta.client.head_bucket(Bucket=name)
        except ClientError as error:
            if int(error.response['Error']['Code']) == 404:
                return None
            raise

        return self.s3.Bucket(name)

    def create_container(self, name, prop_delay=60):
        """
        Creates a container with a given name

        Args:
            name (str): The name of the container
            prop_delay (int, optional): Time to wait for propagation to occur

        Returns:
            An S3 Bucket if found; None otherwise
        """
        log.info('Creating container: %s', name)
        container = self.s3.Bucket(name)

        kwargs = {}
        region = self.session.region_name

        # The us-east-1 region raises errors if used in the constraint
        # https://github.com/boto/boto3/issues/125
        if region != 'us-east-1':
            kwargs['CreateBucketConfiguration'] = {
                'LocationConstraint': region
            }

        container.create(**kwargs)
        container.wait_until_exists()

        # S3 may take some time to propagate newly created buckets to EC2.
        # This is generally only a couple seconds but there doesn't seem to be
        # a supported API approach to handling it.
        log.info('Waiting %ss for container "%s" to propagate',
                 prop_delay, name)
        time.sleep(prop_delay)

        return container

    def upload_to_container(self, image_path, container_name, object_name,
                            chunk_size=CHUNK_SIZE, tags=None):
        """
        Uploads an image to a storage container. If the image is a remote URL,
        it will be streamed in chunks. If the file is a compressed .xz file
        it will stream the decompression to the storage container.
        Decompressing from a remote HTTP file is not supported.

        Args:
            image_path (str): Local or remote HTTP path to the source image
            container_name (str): The container to upload to
            object_name (str): The uploaded file name
            chunk_size (int, optional): Size for HTTP stream chunks
            tags (dict, optional): Dictionary of keyword elements to be
            applied as tags on uploaded S3 image.

        Returns:
            An S3 Object
        """
        log.info('Uploading %s to container %s', image_path, container_name)
        log.info('Uploading %s with name %s', image_path, object_name)

        # Get or create the container
        container = self.get_container_by_name(container_name)
        if not container:
            container = self.create_container(container_name)

        if image_path.lower().startswith('http'):
            # Stream the upload from a remote URL
            log.info('Opening stream to: %s', image_path)
            resp = requests.get(image_path, stream=True, timeout=30)
            resp.raise_for_status()
            stream = resp.iter_content(chunk_size)
            callback = UploadProgress(container_name, object_name)
            if image_path.endswith(".xz"):
                raise NotImplementedError(
                    "LZMA decompression is not implemented "
                    "for S3 content from an HTTP source")
            self.s3.meta.client.upload_fileobj(stream,
                                               container_name,
                                               object_name,
                                               Callback=callback)
        elif image_path.endswith(".xz"):
            # Stream the decompression to the container file
            # Can take a few minutes to load into memory
            callback = UploadProgress(container_name, object_name)
            log.info("Processing a LZMA compressed file: %s.",
                     os.path.basename(image_path))
            with lzma.open(image_path, "rb") as data:
                self.s3.meta.client.upload_fileobj(data,
                                                   container_name,
                                                   object_name,
                                                   Callback=callback)
        else:
            callback = UploadProgress(container_name, object_name,
                                      filepath=image_path)
            self.s3.meta.client.upload_file(image_path,
                                            container_name,
                                            object_name,
                                            Callback=callback)

        log.info('Waiting for object to exist: %s/%s',
                 container_name, object_name)

        obj = self.s3.Object(container_name, object_name)
        obj.wait_until_exists()

        log.info('Successfully uploaded %s', image_path)

        if tags:
            self.tag_s3_object(container_name, object_name, tags)

        return obj

    @log_request_id
    def publish(self, metadata):
        """
        Takes some metadata about a raw disk image, imports it into AWS and
        makes it available to specific accounts/groups.

        This method relies on unique image/metadata names in order to do the
        least amount of work possible. For instance, if the raw disk image has
        already been uploaded to storage but not yet imported as a snapshot,
        the upload will be skipped.

        Args:
            metadata (AWSPublishingMetadata): Metadata about the image

        Returns:
            An EC2 Image
        """
        log.info('Searching for image: %s', metadata.image_name)
        image = (
            self.get_image_by_name(metadata.image_name) or
            self.get_image_by_tags(metadata.tags)
        )

        if not image:
            log.info('Image does not exist: %s', metadata.image_name)
            log.info('Searching for snapshot: %s', metadata.snapshot_name)
            snapshot = self.get_snapshot_by_name(metadata.snapshot_name)

            if not snapshot:
                log.info('Snapshot does not exist: %s', metadata.snapshot_name)
                log.info('Searching for object: %s/%s',
                         metadata.container, metadata.object_name)
                obj = self.get_object_by_name(metadata.container,
                                              metadata.object_name)

                # Set tags when they're provided
                extra_kwargs = {}
                if metadata.tags:
                    extra_kwargs.update({"tags": metadata.tags})

                if not obj:
                    log.info('Object does not exist: %s', metadata.object_name)
                    obj = self.upload_to_container(metadata.image_path,
                                                   metadata.container,
                                                   metadata.object_name,
                                                   **extra_kwargs)
                else:
                    log.info('Object already exists')

                snapshot = self.import_snapshot(obj,
                                                metadata.snapshot_name,
                                                **extra_kwargs)
                self.share_snapshot(snapshot, metadata.snapshot_name,
                                    metadata.snapshot_account_ids)
            else:
                log.info('Snapshot already exists with id: %s', snapshot.id)

            image = self.register_image(snapshot, metadata)
        else:
            log.info('Image already exists with id: %s', image.id)

        # This is an idempotent operation
        self.share_image(image,
                         accounts=metadata.accounts,
                         groups=metadata.groups)

        log.info('Image published: %s', image.id)

        return image

    def import_snapshot(self, obj, snapshot_name, tags=None):
        """
        Imports a disk image as a snapshot.

        Args:
            obj (Object): An S3 Object to import from
            snapshot_name (str): The name of the new snapshot
            tags (dict, optional): Dictionary of keyword elements to be
            applied as tags on imported snapshot.

        Returns:
            An EC2 Snapshot
        """
        tags = tags or {}
        source = '%s/%s' % (obj.bucket_name, obj.key)
        description = 'cloudimg import of %s' % source

        disk_container = {
            'Description': description,
            'Format': 'raw',
            'UserBucket': {
                'S3Bucket': obj.bucket_name,
                'S3Key': obj.key,
            },
        }

        import_args = {
            'Description': description,
            'DiskContainer': disk_container,
        }

        if self.import_role is not None:
            import_args['RoleName'] = self.import_role

        log.info('Importing snapshot from: %s', source)
        task = self.ec2.meta.client.import_snapshot(**import_args)
        snapshot = self.wait_for_import_snapshot_task(task)

        log.info('Tagging snapshot %s with name: %s',
                 snapshot.id, snapshot_name)

        # Set the name of the snapshot so we may be able to look it up later
        tags.update({"Name": snapshot_name})
        self.tag_snapshot(snapshot, tags)

        return snapshot

    def copy_ami(self, image_id, image_name, image_region,):
        """
        Create copy of an AMI.

        Args:
            image_id (str): AMI Id of the image to copy
            name (str): Name of new image
            region(str): Region for new Image.

        Returns:
            Dict with Image_id of the newly created AMI and requests_id
        """
        resp = self.ec2.meta.client.copy_image(
            SourceImageId=image_id,
            Name=image_name,
            SourceRegion=image_region,
            )
        return resp

    def wait_for_import_snapshot_task(self, task, attempts=480, interval=15):
        """
        Waits for a snapshot import task to complete.

        Args:
            task (dict): Import task details
            attempts (int, optional): Max number of times to poll
            interval (int, optional): Seconds between polling

        Returns:
            An EC2 Snapshot
        """
        task_id = task['ImportTaskId']
        status = ''

        log.info('Waiting for import snapshot task with id: %s', task_id)

        queries = 0
        while status.lower() != 'completed':

            queries += 1
            if queries > attempts:
                raise SnapshotTimeout('Timed out waiting for snapshot import')

            time.sleep(interval)

            rsp = self.ec2.meta.client.describe_import_snapshot_tasks(
                ImportTaskIds=[task_id]
            )

            task = rsp['ImportSnapshotTasks'][0]
            detail = task['SnapshotTaskDetail']
            status = detail['Status']
            status_msg = detail.get('StatusMessage', status)
            snapshot_id = detail.get('SnapshotId')
            progress = detail.get('Progress', 'N/A')

            log.info('Snapshot import progress: %s%% - %s',
                     progress, status_msg)

            if status.lower() == 'error':
                raise SnapshotError(status_msg)

        return self.ec2.Snapshot(snapshot_id)

    def register_image(self, snapshot, metadata):
        """
        Registers a snapshot as an image (AMI).

        Args:
            snapshot (Snapshot): The EC2 Snapshot to register from
            metadata (AWSPublishingMetadata): Metadata about the image

        Returns:
            An EC2 Image
        """

        block_device_mapping = [{
            'DeviceName': metadata.root_device_name,
            'Ebs': {
                'SnapshotId': snapshot.id,
                'VolumeType': metadata.volume_type,
                'DeleteOnTermination': True,
            },
        }]

        # Here we just set the "BootMode" whenever we have a proper
        # value for it. Otherwise we leave it so AWS will use the
        # instance type default.
        #
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ami-boot.html
        optional_kwargs = {}
        if metadata.boot_mode != AWSBootMode.not_set:
            optional_kwargs["BootMode"] = metadata.boot_mode.value

        log.info('Registering image: %s', metadata.image_name)
        image = self.ec2.register_image(
            Name=metadata.image_name,
            Description=metadata.description,
            Architecture=metadata.arch,
            VirtualizationType=metadata.virt_type,
            RootDeviceName=metadata.root_device_name,
            BlockDeviceMappings=block_device_mapping,
            EnaSupport=metadata.ena_support,
            SriovNetSupport=metadata.sriov_net_support,
            BillingProducts=metadata.billing_products,
            **optional_kwargs,
        )
        if metadata.tags:
            self.tag_image(image, metadata.tags)
        return image

    def share_image(self, image, accounts=[], groups=[]):
        """
        Shares an image with other user accounts or groups.

        Args:
            image (Image): An EC2 Image
            accounts (list, optional): Names of accounts to share with
            groups (list, optional): Names of groups to share with
        """
        if not accounts and not groups:
            return

        log.info('Sharing %s with accounts: %s', image.name, accounts)
        log.info('Sharing %s with groups: %s', image.name, groups)

        attrs = {
            'LaunchPermission': {
                'Add': [{'UserId': u} for u in accounts] +
                       [{'Group': g} for g in groups]
            }
        }

        image.modify_attribute(**attrs)

    def share_snapshot(self, snapshot, snapshot_name, accounts):
        """
        Shares a snapshot with other user accounts.

        Args:
            snapshot (Snapshot): An EC2 Snapshot
            snapshot_name (str): Name given to the snapshot on creation
            accounts (list, optional): Names of accounts to share with
        """
        if not accounts:
            return

        log.info('Sharing %s with accounts: %s', snapshot_name, accounts)

        attrs = {
            'Attribute': 'createVolumePermission',
            'CreateVolumePermission': {
                'Add': [{'UserId': u} for u in accounts]
            }
        }
        snapshot.modify_attribute(**attrs)

    def tag_s3_object(self, container_name, object_name, tags):
        """
        Apply the corresponding tags to an existing S3 Object.

        Args:
            container_name (str): the name of the S3 bucket
            object_name (str): the object name inside S3 bucket
            tags (dict): Dictionary with the tags to apply
        Returns:
            dict with the versionId of the object the tag-set was added to.
        """
        log.info("Applying tags for object %s/%s: %s",
                 container_name, object_name, tags)
        res = self.s3.meta.client.put_object_tagging(
            Bucket=container_name,
            Key=object_name,
            Tagging={
                "TagSet": self._get_tagdict(tags)
            },
            RequestPayer='requester',
        )
        log.debug("Tag object \"%s/%s\" results: %s",
                  container_name, object_name, res)
        return res

    def tag_snapshot(self, snapshot, tags):
        """
        Apply the corresponding tags to a snapshot.

        Args:
            snapshot (Snapshot): The snapshot to apply the tags
            tags (dict): Dictionary with the tags to apply
        Returns:
            A list of tag resources
        """
        log.info("Applying tags for snapshot %s: %s", snapshot.id, tags)
        res = snapshot.create_tags(
            Tags=self._get_tagdict(tags)
        )
        log.debug("Tag snapshot \"%s\" results: %s", snapshot.id, res)
        return res

    def tag_image(self, image, tags):
        """
        Apply the corresponding tags to an image

        Args:
            image (Image): An EC2 Image
            tags (dict): Dictionary with the tags to apply

        Returns:
            A list of tag resources
        """
        log.info('Tagging image: %s with %s', image.name, tags)

        # AWS expects a list of dicionaries for each tag
        attrs = {
            'DryRun': False,
            'Tags': self._get_tagdict(tags)
        }
        res = image.create_tags(**attrs)
        log.debug("Tag image \"%s\" results: %s", image.name, res)
        return res

    def deregister_image(self, image):
        """
        Deregisters AMI image from AWS.

        Args:
            image (Image): An EC2 Image

        Returns:
            Id of deregistered image. (str)
        """
        image_id, image_name = image.id, image.name
        logging.info("Deregistering image %s (%s)", image_id, image_name)

        image.deregister()

        logging.debug("Deregister image %s (%s)",
                      image_id, image_name)

        return image_id

    def delete_snapshot(self, snapshot):
        """
        Deletes snapshot from AWS.

        Args:
            snapshot (Snapshot): An EC2 Snapshot

        Returns:
            Id of deleted snapshot. (str)
        """
        snapshot_id = snapshot.id
        logging.info("Deleting snapshot %s", snapshot_id)

        snapshot.delete()

        logging.debug("Deleted snapshot %s", snapshot_id)

        return snapshot_id

    @log_request_id
    def delete(self, metadata):
        """
        Deletes AMI images and snapshot for given metadata.

        If specified image is not found, we try to find snapshot
        by other provided metadata.

        Args:
            metadata (AWSDeleteMetadata): Metadata about the image

        Returns:
            tuple (image_id[Str], snapshot_id[Str]]) of removed objects.
        """
        deleted_image_id = None
        deleted_snapshot_id = None

        log.info('Searching for image: %s', metadata.image_id)
        image = (
            self.get_image_by_id(metadata.image_id) or
            self.get_image_by_name(metadata.image_name)
        )

        if image:
            snapshot_id = None
            # extract snapshot_id from existing image
            if image.block_device_mappings:
                snapshot_id = image.block_device_mappings[0]\
                              .get("Ebs", {}).get("SnapshotId") or None

            if snapshot_id is None:
                log.info('Image %s does not reference related snapshot')

            snapshot = self.get_snapshot_by_id(snapshot_id)
            deleted_image_id = self.deregister_image(image)

        # image doesn't exist, let's try to find snapshot
        # by other provided metadata
        else:
            log.info('Image does not exist: %s', metadata.image_id)
            log.info('Searching for snapshot: %s', metadata.snapshot_id)
            snapshot = (
                self.get_snapshot_by_id(metadata.snapshot_id) or
                self.get_snapshot_by_name(metadata.snapshot_name)
            )
            if not snapshot:
                log.info('Snapshot (%s) does not exist', metadata.snapshot_id)

        if snapshot:
            if metadata.skip_snapshot:
                log.info("Skipping snapshot (%s) deletion because "
                         "skip_snapshot is set to True", snapshot.id)
            else:
                deleted_snapshot_id = self.delete_snapshot(snapshot)

        return deleted_image_id, deleted_snapshot_id

    @staticmethod
    def _get_tagdict(tags):
        return [
            {
                "Key": k,
                "Value": v,
            } for k, v in tags.items()
        ]
