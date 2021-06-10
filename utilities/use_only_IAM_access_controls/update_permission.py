# Copyright 2019-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3

session = boto3.Session()
glue = session.client('glue')
lakeformation = session.client('lakeformation')

iam_allowed_principal = {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}

# 1. Modify Data Lake Settings
print('1. Modifying Data Lake Settings to use IAM Controls only...')
data_lake_setting = lakeformation.get_data_lake_settings()['DataLakeSettings']
data_lake_setting['CreateDatabaseDefaultPermissions'] = [{'Principal': iam_allowed_principal, 'Permissions': ['ALL']}]
data_lake_setting['CreateTableDefaultPermissions'] = [{'Principal': iam_allowed_principal, 'Permissions': ['ALL']}]
lakeformation.put_data_lake_settings(DataLakeSettings=data_lake_setting)

# 2. De-register all the data lake locations
print('2. De-registering all the data lake locations...')
res = lakeformation.list_resources()
resources = res['ResourceInfoList']
while 'NextToken' in res:
    res = lakeformation.list_resources(NextToken=res['NextToken'])
    resources.extend(res['ResourceInfoList'])
for r in resources:
    print(f"... Deregistering {r['ResourceArn']} ...")
    lakeformation.deregister_resource(ResourceArn=r['ResourceArn'])

# 3. Grant CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog
print('3. Granting CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog...')
catalog_resource = {'Catalog': {}}
lakeformation.grant_permissions(Principal=iam_allowed_principal,
                                Resource=catalog_resource,
                                Permissions=['CREATE_DATABASE'],
                                PermissionsWithGrantOption=[])

# 4. Grant ALL to IAM_ALLOWED_PRINCIPALS for existing databases and tables
print('4. Granting ALL to IAM_ALLOWED_PRINCIPALS for existing databases and tables...')
databases = []
get_databases_paginator = glue.get_paginator('get_databases')
for page in get_databases_paginator.paginate():
    databases.extend(page['DatabaseList'])
for d in databases:
    print(f"... Granting permissions on database {d['Name']} ...")

    # 4.1. Grant ALL to IAM_ALLOWED_PRINCIPALS for existing databases
    database_resource = {'Database': {'Name': d['Name']}}
    lakeformation.grant_permissions(Principal=iam_allowed_principal,
                                    Resource=database_resource,
                                    Permissions=['ALL'],
                                    PermissionsWithGrantOption=[])

    # 4.2. Update CreateTableDefaultPermissions of database
    location_uri = d.get('LocationUri')
    if location_uri is not None and location_uri != '':
        database_input = {
            'Name': d['Name'],
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
            'Name': d['Name'],
            'Description': d.get('Description', ''),
            'Parameters': d.get('Parameters', {}),
            'CreateTableDefaultPermissions': [
                {
                    'Principal': iam_allowed_principal,
                    'Permissions': ['ALL']
                }
            ]
        }
    glue.update_database(Name=d['Name'],
                         DatabaseInput=database_input)

    # 4.3. Grant ALL to IAM_ALLOWED_PRINCIPALS for existing tables
    tables = []
    get_tables_paginator = glue.get_paginator('get_tables')
    for page in get_tables_paginator.paginate(DatabaseName=d['Name']):
        tables.extend(page['TableList'])

    for t in tables:
        print(f"... Granting permissions on table {d['Name']} ...")
        table_resource = {'Table': {'DatabaseName': d['Name'], 'Name': t['Name']}}
        lakeformation.grant_permissions(Principal=iam_allowed_principal,
                                        Resource=table_resource,
                                        Permissions=['ALL'],
                                        PermissionsWithGrantOption=[])

# 5. Revoke all the permissions except IAM_ALLOWED_PRINCIPALS
print('5. Revoking all the permissions except IAM_ALLOWED_PRINCIPALS...')
res = lakeformation.list_permissions()
permissions = res['PrincipalResourcePermissions']
while 'NextToken' in res:
    res = lakeformation.list_permissions(NextToken=res['NextToken'])
    permissions.extend(res['PrincipalResourcePermissions'])
for p in permissions:
    if p['Principal']['DataLakePrincipalIdentifier'] != 'IAM_ALLOWED_PRINCIPALS':
        print(f"... Revoking permissions of {p['Principal']['DataLakePrincipalIdentifier']} on resource {p['Resource']} ...")
        try:
            lakeformation.revoke_permissions(Principal=p['Principal'],
                                             Resource=p['Resource'],
                                             Permissions=p['Permissions'],
                                             PermissionsWithGrantOption=p['PermissionsWithGrantOption'])
        except Exception as e:
            print(e)


print("Completed!")
