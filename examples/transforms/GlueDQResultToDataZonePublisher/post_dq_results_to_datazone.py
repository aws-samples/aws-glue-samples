"""
This script is an example of code that can be used to create an AWS Glue custom visual transform 
to post data quality results generated by AWS Glue DataQuality in an ETL job to Amazon DataZone.

Dependencies:
- boto3 (AWS SDK for Python)
- awsglue (AWS Glue Python library)
- re (Python regular expression module)
- json (Python JSON module)
- datetime (Python datetime module)

Author: Fabrizio Napolitano
Version: 1.0
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""

import boto3
import re
import json
import pyspark
from datetime import datetime
from awsglue import DynamicFrame

def post_dq_results_to_datazone(self, roleToAssume: str, dzDomain: str, tableName: str, schemaName: str, dqRuleSetName: str, maxResults: int ) -> str:
    """
    Post data quality results to Amazon DataZone.

    Args:
        self (DynamicFrame) : The instance of the class on which this method is called.
        roleToAssume (str): The ARN of the IAM role to assume.
        dzDomain (str): The Amazon DataZone domain identifier.
        tableName (str): The name of the table.
        schemaName (str): The name of the schema.
        dqRuleSetName (str): The name of the ruleset.
        maxResults (int): The maximum number of asset IDs to consider.

    Returns:
        str: A success or error message.
    """

    
    get_logger().info('Starting post data quality results to datazone...')
    try:
        # Get the data quality results from the DynamicFrame
        df =self.toDF()
        dq_results = df.collect()
        
        get_logger().info(f"Data quality results: {dq_results}")

        # Create the Amazon DataZone client
        datazone = create_datazone_client(roleToAssume)
        get_logger().info(f'DataZone Client ready!')

        # Search for the asset ID
        entity_identifier_list = search_asset_id(datazone, dzDomain, tableName, schemaName,maxResults)
        
        get_logger().info(f'list pf entity identifiers: {entity_identifier_list}')

        if entity_identifier_list is None:
            get_logger().error("Error: No asset found")
            return 'error no asset found'
        else:
            # Post the data quality results to Amazon DataZone
            ts_form=generate_ts_form(dq_results,dqRuleSetName)
            response = post_data_quality_results(datazone, dzDomain, entity_identifier_list, ts_form)
            get_logger().info(f"Data quality results posted successfully: {response}")
            return self
    except Exception as e:
        get_logger().error(f"Error posting data quality results: {e}")
        raise DataQualityJobError(f"Error calling post data quality results: {e}")

class DataQualityJobError(Exception):
    """
    Custom exception class for errors related to the data quality job.
    """
    pass  

def create_datazone_client(roleToAssume: str) -> boto3.client:
    """
    Create an Amazon DataZone client with the specified role.

    Args:
        roleToAssume (str): The ARN of the IAM role to assume.
            
        This parameter is needed when running cross accounts and is used to assume a different role in the DataZone Domain Account for the client.
        The reason why we need to assume a different role is because the Datazone post_time_series_data_points does not 
        yet support cross account writes. When the cross account write will be supported for the API we can simplify the 
        method and remove this parameter.

    Returns:
        boto3.client: The Amazon DataZone client.
    """
    try:
        credentials = get_credential_for_role(roleToAssume)
        if not credentials :
            # Create the DataZone client with the local role
            datazone_client = boto3.client('datazone')
        else:
            # Create the DataZone client with the assumed role credentials
            datazone_client = boto3.client(
                service_name='datazone',
                region_name=get_current_region(),
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
                )
    except Exception as e:
        get_logger().error(f"Error creating DataZone client: {e}")
        raise DataQualityJobError(f"Error creating DataZone client: {e}")

    return datazone_client

def get_credential_for_role(roleToAssume: str) -> dict:
    """
    Validate the format of an Amazon Resource Name (ARN).

    Args:
        arn_str (str): The ARN string to validate.

    Returns:
        dict: Dictionary containing the temporary credentials for the assumed role.
    """
    try:
        # Assume the specified IAM role in the domain account
        sts = boto3.client('sts', region_name=get_current_region())                
        assumed_role_object = sts.assume_role(RoleArn=roleToAssume, RoleSessionName="AssumeRoleSession")
        credentials = assumed_role_object['Credentials']
    except Exception as e:
        get_logger().error(f"Error assuming role: {e}; proceding with empty credentials and current role...")
        credentials = {}
    
    return credentials

def get_current_region() -> str:
    """
    Get the current AWS region.

    Returns:
        str: The current AWS region.
    """
    try:
        session = boto3.Session()
        return session.region_name
    except Exception as e:
        get_logger().error(f"Error getting current region: {e}")
        raise DataQualityJobError(f"Error getting current region: {e}")

def search_asset_id(datazone, dzDomain, tableName, schemaName, maxResults: int) -> str:
    """
    Search for an asset in Amazon DataZone.

    Args:
        datazone (boto3.client): The Amazon DataZone client.
        dzDomain (str): The domain identifier.
        tableName (str): The name of the table.
        schemaName (str): The name of the schema.
        maxResults (int): The maximum number of results to return.

    Returns:
        list: The list of entity identifiers for the asset, or None if not found.
    """
    get_logger().info(f'starting search ... ')
        
    entity_identifier_list=[]
        
    try:
        response = datazone.search_listings(
            additionalAttributes=['FORMS'],
            domainIdentifier=dzDomain,
            maxResults=maxResults,
            searchText=tableName
        )

        for item in response['items']:
            forms_dict = json.loads(item['assetListing']['additionalAttributes']['forms'])
            if ('RedshiftTableForm' in forms_dict and
                forms_dict['RedshiftTableForm']['schemaName'] == schemaName and
                forms_dict['RedshiftTableForm']['tableName'] == tableName) or \
                ('GlueTableForm' in forms_dict and
                f"table/{schemaName}/{tableName}" in forms_dict['GlueTableForm']['tableArn']):
                entity_identifier=item['assetListing']['entityId']
                get_logger().info(f"DZ Asset Id: {entity_identifier_list}")
                entity_identifier_list.append(entity_identifier)
            else:
                get_logger().info(f'No matching asset found in this iteration')
            
        get_logger().info(f"DZ Asset Id list: {entity_identifier_list}")
        return entity_identifier_list
    except Exception as e:
        get_logger().error(f"Error searching for asset ID: {e}")
        raise DataQualityJobError(f"Error searching for asset ID: {e}")

def generate_ts_form(results,dqRuleSetName) -> dict:
    """
    Generate the time series form for posting to Amazon DataZone.

    Args:
        results (list): A list of evaluation objects.

    Returns:
        dict: The time series form dictionary.
    """
    try:
        ts_form = {
            "content": json.dumps({
                "evaluationsCount": len(results),
                "evaluations": [process_evaluation(evaluation) for evaluation in results],
                "passingPercentage": calculate_score(results)
            }),
            "formName": dqRuleSetName,  # Specify your desired Ruleset Name
            "typeIdentifier": "amazon.datazone.DataQualityResultFormType",
            "timestamp": datetime.now().timestamp()
        }
            
        get_logger().info(f"Generated time series form: {ts_form}")
        return ts_form
    except Exception as e:
        get_logger().error(f"Error generating time series form: {e}")
        raise DataQualityJobError(f"Error generating time series form: {e}")

def process_evaluation(evaluation) -> dict:
    """
    Process the evaluation results and extract relevant information.

    Args:
        evaluation (object): An evaluation object containing the rule, metrics, and outcome.

    Returns:
        dict: A dictionary containing the processed evaluation information.
    """
    try:
        result = {}
        evaluation_detail_type = "EVALUATION_MESSAGE"
        result_value = evaluation.FailureReason

        # Extract rule, metric types, and columns from the evaluation
        rule, metric_types, columns = extract_metadata(evaluation.Rule, evaluation.EvaluatedMetrics)

        evaluation_details = {evaluation_detail_type: result_value} if result_value else {}

        result = {
            "applicableFields": columns,
            "types": metric_types,
            "status": "PASS" if evaluation.Outcome == "Passed" else "FAIL",
            "description": f"{rule}",
            "details": evaluation_details
        }
            
        get_logger().info(f"Processed evaluation: {result}")

        return result
    except Exception as e:
        get_logger().error(f"Error processing evaluation: {e}")
        raise DataQualityJobError(f"Error processing evaluation: {e}")

def extract_metadata(rule, evaluated_metrics):
    """
    Extract information on rule, metric types, and columns from the evaluation.

    Args:
        rule (str): The rule description.
        evaluated_metrics (dict): The evaluated metrics.

    Returns:
        tuple: A tuple containing the rule, metric types, and columns.
    """
    try:
        metric_types = [key.split(".")[-1] for key in evaluated_metrics]
        columns = re.findall(r'"(.*?)"', rule)
            
        get_logger().info(f"Extracted metadata: rule={rule}, metric_types={metric_types}, columns={columns}")
        return rule, metric_types, columns
    except Exception as e:
        get_logger().error(f"Error extracting metadata: {e}")
        raise DataQualityJobError(f"Error extracting metadata: {e}")

def calculate_score(results) -> float:
    """
    Calculate the score based on the number of successful evaluations.

    Args:
        results (list): A list of evaluation objects.

    Returns:
        float: The calculated score as a percentage.
    """
    try:
        num_success = sum(1 for evaluation in results if evaluation.Outcome == "Passed")
        total_results = len(results)
            
        score = (num_success / total_results) * 100 if total_results else 0.0
        get_logger().info(f"Calculated score: {score}%")
        return score
    except Exception as e:
        get_logger().error(f"Error calculating score: {e}")
        raise DataQualityJobError(f"Error calculating score: {e}")

def post_data_quality_results(datazone, dzDomain, entity_identifier_list, ts_form) -> dict:
    """
    Post data quality time series data points to Amazon DataZone.

    Args:
        datazone (boto3.client): The Amazon DataZone client.
        entity_identifier (str): The asset identifier.
        ts_form (dict): The time series form dictionary.

    Returns:
        List of dict: The list of responses from each of the post_time_series_data_points API call.
    """
    response_list=[]
        
    try:
        for ei in entity_identifier_list:
            response= datazone.post_time_series_data_points(
                domainIdentifier=dzDomain,
                entityIdentifier=ei,
                entityType='ASSET',
                forms=[ts_form]
            )
            
            get_logger().info(f"Posted results: {response}")
            response_list.append(response)
            
        return response_list
    except Exception as e:
        get_logger().error(f"Error posting data quality results: {e}")
        raise DataQualityJobError(f"Error posting data quality results: {e}")
    
def get_logger():
    return pyspark.SparkContext.getOrCreate()._jvm.com.amazonaws.services.glue.log.GlueLogger()


DynamicFrame.post_dq_results_to_datazone = post_dq_results_to_datazone