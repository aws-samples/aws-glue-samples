#!/usr/bin/env bash
# Script to create new container with the supplied jars and to push the container to the created ECR repository
     
 # Steps to run the script :
 # 1) chmod +x ./container_setup.sh
 # 2) ./container_setup.sh --jar-file-paths [Paths to jar file] --jar-folder-paths [Paths to folder where jars are present] --aws-region [AWS Region] --ecr-repo-name [ECR repo name] --aws-account-id [AWS Account ID] --aws-cli-profile [Local AWS CLI profile name] --image-tag [image tag]
 
 # Example 1: ./container_setup.sh --aws-account-id [AWS Account ID] --ecr-repo-name my-company/salesforce --aws-region us-west-1 --jar-file-paths /Users/AWS/Desktop/local_repo/partner.jdbc.salesforce.jar --jar-folder-paths /Users/AWS/Desktop/local_repo/ --image-tag 1.0.0
 # Example 2: ./container_setup.sh --aws-account-id [AWS Account ID] --ecr-repo-name my-company/salesforce --aws-region us-west-1 --jar-file-paths partner.jdbc.salesforce.jar --jar-folder-paths /Users/AWS/Desktop/local_repo/ --aws-cli-profile user1 --image-tag 1.0.0
 # Example 3: ./container_setup.sh --aws-account-id [AWS Account ID] --ecr-repo-name my-compnay/salesforce --aws-region us-west-1 --jar-file-paths /Users/AWS/Desktop/local_repo/partner.jdbc.salesforce.jar,partner.jdbc.salesforce.jar --jar-folder-paths /Users/AWS/Desktop/local_repo/ --aws-cli-profile user1 --image-tag 1.0.0
 set -x

 ecr_repo_name=""
 aws_region=""
 aws_account_id=""
 jarfile_paths=""
 jarfolder_paths=""
 aws_cli_profile=""
 config_path=""
 image_tag=""
 
 # call_help() function to give example of how to give the parameters
call_help(){
  echo "
     Steps to run the script :
     
     1) chmod +x ./container_setup.sh
     2) ./container_setup.sh --jar-file-paths [Paths to jar file] --jar-folder-paths [Paths to folder where jars are present] --aws-region [AWS Region] --ecr-repo-name [ECR repo name] --aws-account-id [AWS Account ID] --image-tag [image tag]
     
     Example 1: ./container_setup.sh --aws-account-id [AWS Account ID] --ecr-repo-name my-company/salesforce --aws-region us-west-1 --jar-file-paths /Users/AWS/Desktop/local_repo/partner.jdbc.salesforce.jar --jar-folder-paths /Users/AWS/Desktop/local_repo/ --image-tag 1.0.0
     
     Example 2: ./container_setup.sh --aws-account-id [AWS Account ID] --ecr-repo-name my-company/salesforce --aws-region us-west-1 --jar-file-paths partner.jdbc.salesforce.jar --jar-folder-paths /Users/AWS/Desktop/local_repo/ --image-tag 1.0.0
     
     Example 3: ./container_setup.sh --aws-account-id [AWS Account ID] --ecr-repo-name my-company/salesforce --aws-region us-west-1 --jar-file-paths /Users/AWS/Desktop/local_repo/partner.jdbc.salesforce.jar,partner.jdbc.salesforce.jar --jar-folder-paths /Users/AWS/Desktop/local_repo/ --image-tag 1.0.0 --aws-cli-profile user1
     
     Note: You have a choice to give only --jar-file-paths or --jar-folder-paths or you can give both the parameters. But one parameter is required.
     
     --jar-file-paths        Optional Field      Paths to jar file (Refer Note) (Can give multiple jar file paths eg: /usr/jarfile1,/usr/jarfile2)
     --jar-folder-paths      Optional Field      Paths to folder where jar is present (Refer Note) (Can give multiple jar folder paths eg: /usr/jarfolder1/,/usr/jarfolder2/,/usr/jarfolder3)
     --aws-account           Required Field      AWS Account ID
     --ecr-repo-name         Required Field      ECR repository name
     --aws-region            Required Field      AWS Region
     --aws-cli-profile       Optional Field      Local AWS CLI profile name (If no AWS CLI profile name given, it will take the default AWS CLI profile)
     --config-path           Required Field      config file that describes key attributes about the connector jar file, read the usage guide doc to see how to prepare the config file.
     --image-tag             Required Field      tag for the image to be pushed to ECR repo, this should be the same as the version of the connector you're publishing.
  "
 }
 
 # Call help menu if --help passed
 if [[ (${#@} == 1) && ("$1" == "--help") ]]; then
   call_help
   exit 1
 fi

  # To create a Dockerfile
cat > Dockerfile <<- "EOF"
FROM amazonlinux:2.0.20210721.2
COPY ./connector_jars  ./jars
EOF
 
 
 # Create a new directory for the connector jars
 mkdir -p connector_jars

 # Clean up the directory for connector jars
 rm -f connector_jars/* 
 
 # Check the total number of parameters passed to the shell script
 if [[ (${#@} != 10 && ${#@} != 12) && (${#@} != 14) ]]; then
   echo "Invalid parameters given"
   call_help
   exit 1
 fi
 
 # Checking and Initialising variables based on given parameters and flags
 while [[ $# -ne 0 ]]; do
   if [ "$1" == "--ecr-repo-name" ] ; then
     ecr_repo_name="$2"
     shift 2
   elif [ "$1" == "--aws-region" ] ; then
     aws_region="$2"
     shift 2
   elif [ "$1" == "--aws-account-id" ] ; then
     aws_account_id="$2"
     shift 2
   elif [ "$1" == "--jar-file-paths" ] ; then
     jarfile_paths="$2"
     shift 2
   elif [ "$1" == "--jar-folder-paths" ] ; then
     jarfolder_paths="$2"
     shift 2
   elif [ "$1" == "--aws-cli-profile" ] ; then
     aws_cli_profile="$2"
     shift 2
   elif [ "$1" == "--config-path" ] ; then
     config_path="$2"
     shift 2
   elif [ "$1" == "--image-tag" ] ; then
     image_tag="$2"
     shift 2
   else
     echo "Invalid Parameters"
       call_help
       exit 1
   fi
 done
 
 re='^[0-9]+$'
 
 # Check the parameters passed
 
 # To check whether the parameter contains only number, then it AWS Account ID
 if [[ $aws_account_id =~ $re ]]; then
   if [[ ${#aws_account_id} = 12 ]]; then
       echo "Accepted ${aws_account_id} as AWS Account ID"
   else
       echo "Given ${aws_account_id} is an invalid AWS Account ID, expected a 12-digit number"
       exit 1
   fi
 fi
 
 # To check whether the parameter is a path to a connector jars file and copying connector jars to connector_jars folder
 IFS=','
 read -a str_arr_folder <<< "$jarfolder_paths"
 for folder_name in "${str_arr_folder[@]}";
 do
   if [[ -d $folder_name ]]; then
     folder_path_length=${#folder_name}
     suffix=${folder_name:folder_path_length-1:folder_path_length}
     slash="/"
     if [[ $suffix == $slash ]]; then
       cp $folder_name*.jar ./connector_jars
     else
       cp $folder_name/*.jar ./connector_jars
     fi
   else
     echo "$folder_name is not a valid Directory"
     exit 1
   fi
 done
 IFS=' '
 
 # To check whether the parameter is a path to a connector jars folder
 IFS=','
 read -a str_arr_file <<< "$jarfile_paths"
 for file_name in "${str_arr_file[@]}";
 do
   if [[ -f $file_name ]]; then
     size=${#file_name}
     check=${file_name:size-4:size}
     name=".jar"
     if [[ $check == $name ]]; then
       cp $file_name ./connector_jars
     fi
   else
     echo "$file_name is not a valid File"
     exit 1
   fi
 done
 IFS=' '

 # check if --config-path is supplied, copy the config file into the dest folder if yes.
 if [ -z "$config_path" ]
 then
   echo "--config-path is required to run the script, exiting..."
   exit 1
 else
   cp $config_path ./connector_jars/config.json
 fi

 
 # To check whether the given parameter is an AWS Region
 IFS='-'
 read -a split_region <<< "$aws_region"
 if [[ (${#split_region[*]} == 3 && ${#split_region[2]} == 1) && (${#split_region[0]} == 2 && ${split_region[2]} =~ $re) ]]; then
   IFS=' '
   echo "Accepted $aws_region as AWS Region"
 else
   echo "Given $aws_region is an Invalid AWS Region"
   exit 1
 fi
 
 # To Authenticate to your default registry
 if [ -z "$aws_cli_profile" ]
 then
   # docker_login=$(aws ecr get-login --no-include-email --region $aws_region)
   # ret=$(docker_login)
   aws ecr get-login-password --region $aws_region | docker login --username AWS --password-stdin $aws_account_id.dkr.ecr.$aws_region.amazonaws.com
 else
   aws ecr get-login-password --region $aws_region --profile $aws_cli_profile | docker login --username AWS --password-stdin $aws_account_id.dkr.ecr.$aws_region.amazonaws.com
 fi
 
 # If login succeeded, we get an error code 0
 if [ $? -eq "0" ]
 then
    echo "Login Succeeded"
    if [ -z "$aws_cli_profile" ]
    then
      aws ecr describe-repositories --repository-names $ecr_repo_name --region $aws_region || aws ecr create-repository --repository-name $ecr_repo_name --region $aws_region
    else
      aws ecr describe-repositories --repository-names $ecr_repo_name --region $aws_region --profile $aws_cli_profile || aws ecr create-repository --repository-name $ecr_repo_name --region $aws_region --profile $aws_cli_profile
    fi
    docker build -t connector:$image_tag .
    docker tag connector:$image_tag $aws_account_id.dkr.ecr.$aws_region.amazonaws.com/$ecr_repo_name:$image_tag
    docker push $aws_account_id.dkr.ecr.$aws_region.amazonaws.com/$ecr_repo_name:$image_tag
 else
    echo "Login Failed due to Invalid AWS Credentials or Invalid AWS Region"
 fi
