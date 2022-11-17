# Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import boto3
import argparse
import logging
from distutils.util import strtobool

# Configure credentials and required parameters
parser = argparse.ArgumentParser()
parser.add_argument('-p', '--profile', dest='profile', type=str, help='AWS named profile name')
parser.add_argument('-r', '--region', dest='region', type=str, help='AWS Region (e.g. "us-east-1")')
parser.add_argument('-g', '--global', dest='global_conf', type=strtobool, default=True, help='(Optional) Update global configurations; Data Lake Settings and Data lake locations (default: true)')
parser.add_argument('-d', '--databases', dest='target_databases', type=str, help='(Optional) The comma separated list of target database names (default: all databases)')
parser.add_argument('--skip-errors', dest='skip_errors', action="store_true", help='(Optional) Skip errors and continue execution. (default: false)')
parser.add_argument('--dryrun', dest='dryrun', action="store_true", help='(Optional) Display the operations that would be performed using the specified command without actually running them (default: false)')
parser.add_argument('-v', '--verbose', dest='verbose', action="store_true", help='(Optional) Display verbose logging (default: false)')
args = parser.parse_args()

logger = logging.getLogger()
logger_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(logger_handler)
if args.verbose:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
for libname in ["boto3", "botocore", "urllib3"]:
    logging.getLogger(libname).setLevel(logging.WARNING)

session_args = {}
if args.profile is not None:
    session_args['profile_name'] = args.profile
    logger.info(f"boto3 Session uses {args.profile} profile based on the argument.")
if args.region is not None:
    session_args['region_name'] = args.region
    logger.info(f"boto3 Session uses {args.region} region based on the argument.")

target_databases = []
if args.target_databases is not None:
    target_databases = args.target_databases.split(',')
    args.global_conf = False
    logger.info("Disabled updates for global configurations since the parameter '--database' is given.")

session = boto3.Session(**session_args)
sts = session.client('sts')
account_id = sts.get_caller_identity().get('Account')
argument_setting_msg = f"- Account: {account_id}\n- Profile: {session.profile_name}\n- Region: {session.region_name}"
if target_databases:
    argument_setting_msg += f"\n- Databases: {target_databases}"
logger.info(argument_setting_msg)

do_update = not args.dryrun


def prompt(message):
    answer = input(message)
    if answer.lower() in ["n","no"]:
        sys.exit(0)
    elif answer.lower() not in ["y","yes"]:
        prompt(message)

if do_update:
    prompt(f"Are you sure to make modifications on Lake Formation permissions to use only IAM access control? (y/n): ")
else:
    logger.info("Running in dry run mode. There are no updates triggered by this execution.")

glue = session.client('glue')
lakeformation = session.client('lakeformation')

iam_allowed_principal = {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}


def update_data_lake_settings():
    """Function to update data lake settings to use IAM access control only

    """
    logger.info('1. Modifying Data Lake Settings to use IAM Controls only...')
    data_lake_setting = lakeformation.get_data_lake_settings()['DataLakeSettings']
    data_lake_setting['CreateDatabaseDefaultPermissions'] = [{'Principal': iam_allowed_principal, 'Permissions': ['ALL']}]
    data_lake_setting['CreateTableDefaultPermissions'] = [{'Principal': iam_allowed_principal, 'Permissions': ['ALL']}]
    if do_update:
        lakeformation.put_data_lake_settings(DataLakeSettings=data_lake_setting)
    logger.info('Done.')


def deregister_data_lake_locations():
    """Function to de-register all the data lake locations

    """
    logger.info('2. De-registering all the data lake locations...')
    res = lakeformation.list_resources()
    resources = res['ResourceInfoList']
    while 'NextToken' in res:
        res = lakeformation.list_resources(NextToken=res['NextToken'])
        resources.extend(res['ResourceInfoList'])
    for r in resources:
        logger.info(f"... De-registering {r['ResourceArn']} ...")
        if do_update:
            lakeformation.deregister_resource(ResourceArn=r['ResourceArn'])
    logger.info('Done.')


def grant_db_perm_to_iam_allowed_principals():
    """Function to grant CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog

    """
    logger.info('3. Granting CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog...')
    catalog_resource = {'Catalog': {}}

    if do_update:
        try:
            lakeformation.grant_permissions(Principal=iam_allowed_principal,
                                            Resource=catalog_resource,
                                            Permissions=['CREATE_DATABASE'],
                                            PermissionsWithGrantOption=[])
        except Exception as e:
            logger.error(f"Error occurred in the resource: {catalog_resource}")
            if args.skip_errors:
                logger.error(f"Skipping error: {e}", exc_info=True)
            else:
                raise

    logger.info('Done.')


def grant_all_to_iam_allowed_principals_for_database_table():
    """Function to grant ALL to IAM_ALLOWED_PRINCIPALS for existing databases and tables

    """
    logger.info('4. Granting ALL to IAM_ALLOWED_PRINCIPALS for existing databases and tables...')
    databases = []
    get_databases_paginator = glue.get_paginator('get_databases')
    for page in get_databases_paginator.paginate():
        databases.extend(page['DatabaseList'])
    for d in databases:
        database_name = d['Name']

        # Skip database if it is a resource link
        if 'TargetDatabase' in d:
            logger.debug(f"Database '{database_name}' is skipped since it is a resource link.")
            continue
        # Skip database if it is not target database
        if target_databases and database_name not in target_databases:
            logger.debug(f"Database '{database_name}' is skipped since it is not given in --databases option.")
            continue

        # 4.1. Grant ALL to IAM_ALLOWED_PRINCIPALS for existing databases
        logger.info(f"... Granting ALL permissions to IAM_ALLOWED_PRINCIPALS on database '{database_name}' ...")
        database_resource = {'Database': {'Name': database_name}}
        if do_update:
            try:
                lakeformation.grant_permissions(Principal=iam_allowed_principal,
                                                Resource=database_resource,
                                                Permissions=['ALL'],
                                                PermissionsWithGrantOption=[])
            except Exception as e:
                logger.error(f"Error occurred in the resource: {database_resource}")
                if args.skip_errors:
                    logger.error(f"Skipping error: {e}", exc_info=True)
                else:
                    raise

        # 4.2. Update CreateTableDefaultPermissions of database
        location_uri = d.get('LocationUri')
        logger.info(f"... Updating CreateTableDefaultPermissions property on database '{database_name}' ...")
        if location_uri is not None and location_uri != '':
            database_input = {
                'Name': database_name,
                'Description': d.get('Description', ''),
                'LocationUri': location_uri,
                'Parameters': d.get('Parameters', {}),
                'CreateTableDefaultPermissions': [
                    {
                        'Principal': iam_allowed_principal,
                        'Permissions': ['ALL']
                    }
                ]
            }
        else:
            database_input = {
                'Name': database_name,
                'Description': d.get('Description', ''),
                'Parameters': d.get('Parameters', {}),
                'CreateTableDefaultPermissions': [
                    {
                        'Principal': iam_allowed_principal,
                        'Permissions': ['ALL']
                    }
                ]
            }
        if do_update:
            glue.update_database(Name=database_name,
                                 DatabaseInput=database_input)

        # 4.3. Grant ALL to IAM_ALLOWED_PRINCIPALS for existing tables
        tables = []
        get_tables_paginator = glue.get_paginator('get_tables')
        for page in get_tables_paginator.paginate(DatabaseName=database_name):
            tables.extend(page['TableList'])

        for t in tables:
            table_name = t['Name']

            # Skip table if it is a resource link
            if 'TargetTable' in t:
                logger.debug(f"Table '{database_name}.{table_name}' is skipped since it is a resource link.")
                continue

            logger.info(f"... Granting ALL permissions to IAM_ALLOWED_PRINCIPALS on table '{database_name}.{table_name}' ...")
            table_resource = {'Table': {'DatabaseName': database_name, 'Name': table_name}}
            if do_update:
                try:
                    lakeformation.grant_permissions(Principal=iam_allowed_principal,
                                                    Resource=table_resource,
                                                    Permissions=['ALL'],
                                                    PermissionsWithGrantOption=[])
                except Exception as e:
                    logger.error(f"Error occurred in the resource: {table_resource}")
                    if args.skip_errors:
                        logger.error(f"Skipping error: {e}", exc_info=True)
                    else:
                        raise
    logger.info('Done.')


def get_catalog_id(resource):
    """Utility function to retrieve catalog ID from resource for supporting both single/cross-account usage

    Args:
        resource: Dictionary object of the target resource

    Returns:
        string: Catalog id (AWS account ID)

    """
    for key in resource.keys():
        if isinstance(resource[key], dict):
            return get_catalog_id(resource[key])
        elif 'CatalogId' == key:
            return resource[key]


def get_resource_type_name_database(resource):
    """Utility function to retrieve resource type, name and database name from resource

    Args:
        resource: Dictionary object of the target resource

    Returns:
        tuple: Resource type, name and database name (empty if there is no database related to the resource)

    """
    database = ""
    if "Catalog" in resource:
        type = "Catalog"
        name = "catalog"
    elif "Database" in resource:
        type = "Database"
        name = resource[type]['Name']
        database = name
    elif "Table" in resource:
        type = "Table"
        name = f"{resource[type]['DatabaseName']}.{resource[type]['Name']}"
        database = resource[type]['DatabaseName']
    elif "TableWithColumns" in resource:
        type = "TableWithColumns"
        name = f"{resource[type]['DatabaseName']}.{resource[type]['Name']}.columns"
        database = resource[type]['DatabaseName']
    elif "DataLocation" in resource:
        type = "DataLocation"
        name = resource[type]['ResourceArn']
    elif "DataCellsFilter" in resource:
        type = "DataCellsFilter"
        name = f"{resource[type]['DatabaseName']}.{resource[type]['TableName']}.{resource[type]['Name']}"
        database = resource[type]['DatabaseName']
    elif "LFTag" in resource:
        type = "LFTag"
        name = resource[type]['TagKey']
    elif "LFTagPolicy" in resource:
        type = "LFTagPolicy"
        name = resource[type]['Expression']
    else:
        type = "unknown"
        name = "unknown_resource"
        logger.warn(f"Unknown resource type: {resource}")
    return type, name, database


def revoke_all_permissions():
    """Function to revoke all the Lake Formation permissions except IAM_ALLOWED_PRINCIPALS

    """
    logger.info('5. Revoking all the permissions except IAM_ALLOWED_PRINCIPALS...')
    res = lakeformation.list_permissions()
    permissions = res['PrincipalResourcePermissions']
    while 'NextToken' in res:
        res = lakeformation.list_permissions(NextToken=res['NextToken'])
        permissions.extend(res['PrincipalResourcePermissions'])
    for p in permissions:
        if p['Principal']['DataLakePrincipalIdentifier'] != 'IAM_ALLOWED_PRINCIPALS':
            resource = p['Resource']
            resource_type, resource_name, resource_database_name = get_resource_type_name_database(resource)

            # Skip resource if it is not owned by this account
            catalog_id = get_catalog_id(p['Resource'])
            if catalog_id != account_id:
                logger.debug(f"The {resource_type} resource '{resource_name}' is skipped since it is not owned by the account {account_id}.")
                continue

            # Skip resources if it is not included in target databases
            if target_databases:
                if resource_database_name != "" and resource_database_name not in target_databases:
                    logger.debug(f"The {resource_type} resource '{resource_name}' is skipped since it is defined in the database '{resource_database_name}' which is not given in --databases option.")
                    continue
                elif resource_database_name == "":
                    logger.debug(f"The {resource_type} resource '{resource_name}' is skipped since it is not defined for specific database.")
                    continue

            logger.info(f"... Revoking permissions of {p['Principal']['DataLakePrincipalIdentifier']} on the {resource_type} resource '{resource_name}' ...")

            # When `TableWildcard` field exists, remove `Name` field because RevokePermissions API does not accept the resource input having both `Name` and `TableWildcard`.
            if resource_type == "Table" and 'TableWildcard' in p['Resource']['Table']:
                del p['Resource']['Table']['Name']

            # When `ColumnWildcard` field exists for `ALL_TABLES`, replace `TableWithColumns` resource with `Table` resource, and call RevokePermission API with `TableWildcard`.
            if resource_type == "TableWithColumns" and 'ColumnWildcard' in p['Resource']['TableWithColumns'] and 'Name' in p['Resource']['TableWithColumns'] and p['Resource']['TableWithColumns']['Name'] == "ALL_TABLES":
                p['Resource']['Table'] = {}
                p['Resource']['Table']['CatalogId'] = p['Resource']['TableWithColumns']['CatalogId']
                p['Resource']['Table']['DatabaseName'] = p['Resource']['TableWithColumns']['DatabaseName']
                p['Resource']['Table']['TableWildcard'] = {}
                del p['Resource']['TableWithColumns']

            if do_update:
                try:
                    lakeformation.revoke_permissions(Principal=p['Principal'],
                                                     Resource=p['Resource'],
                                                     Permissions=p['Permissions'],
                                                     PermissionsWithGrantOption=p['PermissionsWithGrantOption'])
                except Exception as e:
                    logger.error(f"Error occurred in the argument: {p}")
                    if args.skip_errors:
                        logger.error(f"Skipping error: {e}", exc_info=True)
                    else:
                        raise

    logger.info('Done.')


def main():
    if args.global_conf:
        update_data_lake_settings()
        deregister_data_lake_locations()
        grant_db_perm_to_iam_allowed_principals()
    else:
        logger.info("... Skipping the global configuration since the parameter '--global false' is given ...")

    grant_all_to_iam_allowed_principals_for_database_table()
    revoke_all_permissions()
    logger.info("Completed!")


if __name__ == "__main__":
    main()
