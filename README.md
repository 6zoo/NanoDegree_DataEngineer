
*Tips to Run this Project*
1. set VPC of Redshift Cluster to public accesible for access from the jupyter notebook.

2. Set HOST variable in dwh.cfg file as Endpoint from Redshift cluster dashboard
3. Set Redshift cluster region as same as the S3 bucket(In this Project, Sparkify data is in us-west-2 region)
4. IAM role of the Redshift cluster should have S3 Access available to get copy data from the S3 bucket by proxy
5. You can create redshift cluster and iam role from both AWS Redshift and Codes