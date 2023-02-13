import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from domain.utils import file_in_s3_bucket


class TestFileInS3(unittest.TestCase):

    @patch('domain.utils.appconfig', return_value=MagicMock())
    @patch('domain.utils.s3_bucket', return_value=MagicMock())
    @patch('domain.utils.s3_bucket_name', return_value='test_bucket')
    def test_file_exists(self, mock_name, mock_bucket, mock_client):
        s3_bucket = mock_bucket.return_value
        s3_bucket_name = mock_name.return_value
        s3_bucket.Object.return_value = s3_bucket_name
        file_name = 'test_file_name'
        file_prefix = 'test_prefix'

        result = file_in_s3_bucket(file_name, file_prefix)

        self.assertEqual(True, result)

    @patch('domain.utils.s3_bucket')
    def test_file_in_s3_bucket_does_not_exist(self, mock_s3_object):
        mock_s3_object.Object.return_value.load.side_effect = ClientError(
            {'Error': {'Code': '404', 'Message': 'Not Found'}}, 'operation_name')
        result = file_in_s3_bucket("file_name", "prefix")
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
