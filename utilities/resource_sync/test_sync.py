# Copyright 2019-2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import unittest
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
import sync


class TestSync(unittest.TestCase):

    def setUp(self):
        self.mock_aws = mock_aws()
        self.mock_aws.start()

        self.glue_client = boto3.client('glue', region_name='us-east-1')
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.sts_client = boto3.client('sts', region_name='us-east-1')

        # Mock sys.exit to prevent actual exit during tests
        self.exit_patcher = patch('sys.exit')
        self.mock_exit = self.exit_patcher.start()

        # Mock args
        self.args_patcher = patch('sync.args', MagicMock(
            skip_no_dag_jobs=True,
            overwrite_databases=True,
            skip_errors=False,
            copy_job_script=True,
            dst_region='us-east-1',
            targets='job',
            src_job_names=None,
            config_path=None
        ))
        self.mock_args = self.args_patcher.start()

    def tearDown(self):
        self.mock_aws.stop()
        self.exit_patcher.stop()
        self.args_patcher.stop()

    def test_organize_job_param(self):
        job = {
            'Name': 'TestJob',
            'AllocatedCapacity': 10,
            'CreatedOn': '2023-01-01',
            'Command': {
                'ScriptLocation': 's3://old-bucket/script.py'
            }
        }
        mapping = {
            's3://old-bucket': 's3://new-bucket'
        }
        
        result = sync.organize_job_param(job, mapping)
        
        self.assertNotIn('AllocatedCapacity', result)
        self.assertNotIn('CreatedOn', result)
        self.assertEqual(result['Command']['ScriptLocation'], 's3://new-bucket/script.py')

    def test_copy_job_script(self):
        self.s3_client.create_bucket(Bucket='src-bucket')
        self.s3_client.put_object(Bucket='src-bucket', Key='script.py', Body='print("Hello")')
        
        mock_dst_s3 = MagicMock()
        mock_dst_s3.meta.client = self.s3_client
        
        with patch('sync.src_s3', boto3.resource('s3')), \
            patch('sync.dst_s3_client', self.s3_client), \
            patch('sync.dst_s3', mock_dst_s3), \
            patch('sync.args', MagicMock(dst_region='us-east-1')):
            sync.copy_job_script('s3://src-bucket/script.py', 's3://dst-bucket/script.py')
        
        result = self.s3_client.get_object(Bucket='dst-bucket', Key='script.py')
        self.assertEqual(result['Body'].read().decode('utf-8'), 'print("Hello")')

    def test_synchronize_job(self):
        self.glue_client.create_job(
            Name='TestJob',
            Role='TestRole',
            Command={'Name': 'glueetl', 'ScriptLocation': 's3://src-bucket/script.py'}
        )
        
        with patch('sync.src_glue', self.glue_client), \
             patch('sync.dst_glue', self.glue_client), \
             patch('sync.copy_job_script'):
            sync.synchronize_job('TestJob', {})
        
        job = self.glue_client.get_job(JobName='TestJob')['Job']
        self.assertEqual(job['Name'], 'TestJob')

    def test_synchronize_database(self):
        self.glue_client.create_database(DatabaseInput={'Name': 'TestDB'})
        
        with patch('sync.src_glue', self.glue_client), \
             patch('sync.dst_glue', self.glue_client):
            sync.synchronize_database({'Name': 'TestDB'}, {})
        
        db = self.glue_client.get_database(Name='TestDB')['Database']
        self.assertEqual(db['Name'], 'TestDB')

    def test_synchronize_table(self):
        self.glue_client.create_database(DatabaseInput={'Name': 'TestDB'})
        self.glue_client.create_table(
            DatabaseName='TestDB',
            TableInput={
                'Name': 'TestTable',
                'StorageDescriptor': {
                    'Columns': [{'Name': 'col1', 'Type': 'string'}]
                },
                'TableType': 'EXTERNAL_TABLE'  # Add this line
            }
        )
        
        with patch('sync.src_glue', self.glue_client), \
             patch('sync.dst_glue', self.glue_client):
            sync.synchronize_table({
                'Name': 'TestTable',
                'DatabaseName': 'TestDB',
                'StorageDescriptor': {
                    'Columns': [{'Name': 'col1', 'Type': 'string'}]
                },
                'TableType': 'EXTERNAL_TABLE'  # Add this line
            }, {})
        
        table = self.glue_client.get_table(DatabaseName='TestDB', Name='TestTable')['Table']
        self.assertEqual(table['Name'], 'TestTable')
        self.assertEqual(table['DatabaseName'], 'TestDB')

    def test_main_without_destination_profile(self):
        with patch('sync.parse_arguments') as mock_parse_args, \
            patch('sync.boto3.Session') as mock_session, \
            patch('sync.boto3.client') as mock_client, \
            patch('sync.load_mapping_config_file', return_value={}), \
            patch('sync.initialize') as mock_initialize:
            mock_parse_args.return_value = MagicMock(
                dst_profile=None,
                dst_role_arn=None,
                config_path=None,
                targets='job',
                src_job_names=None
            )
            mock_session.return_value.get_credentials.return_value = None
            
            # Simulate the error condition
            mock_initialize.side_effect = SystemExit(1)
            
            with self.assertRaises(SystemExit) as cm:
                sync.main()
            
            self.assertEqual(cm.exception.code, 1)
            mock_client.assert_not_called()

if __name__ == '__main__':
    unittest.main()
