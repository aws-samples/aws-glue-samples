## Automation for Amazon SageMaker Notebook with Glue Development Endpoint

### Launching the SageMaker Notebook Using AWS CloudFormation

In Glue console, you can create SageMaker Notebook for your Development Endpoint.
   - [Tutorial: Use an Amazon SageMaker Notebook with Your Development Endpoint](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-sage.html).

If you want to automate this process, you can use an AWS CloudFormation template to launch the SageMaker Notebook with Glue Development Endpoint. 
This template is just a sample that you should modify to meet your requirements. 

#### Limitations

- Development Endpoint is created without any additional configuration. 
  If you want to customize the Development Endpoint, you can modify the template, or create a Development Endpoint manually and switch the Development Endpoint in the notebook. 
  
#### China regions

- For China regions, please use [glue_sagemaker_notebook_cn.yaml](./glue_sagemaker_notebook_cn.yaml) as CouldFormation Template.
- For other regions, please use [glue_sagemaker_notebook.yaml](./glue_sagemaker_notebook.yaml) as CouldFormation Template.


