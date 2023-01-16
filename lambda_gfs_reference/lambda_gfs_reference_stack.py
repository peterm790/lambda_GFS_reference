from constructs import Construct

from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_iam as iam,
    aws_events as events,
    Duration
)

from aws_cdk.aws_lambda_event_sources import S3EventSource

from aws_cdk.aws_lambda_python_alpha import PythonFunction


class LambdaGfsReferenceStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        policy = iam.Policy(self, "MyPolicy",
                    policy_name="AllowS3ReadAccess",
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3:GetObject"],
                            resources=["arn:aws:s3:::*"]
                        )
                    ])

        # Create an IAM role that allows access to the S3 bucket
        role = iam.Role(self, "MyRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        # Attach the policy to the role
        policy.attach_to_role(role)


        # This is the lambda that creates reference files using kerchunk
        lambda_reference = _lambda.DockerImageFunction(
            scope = self,
            id = "CreateReference",
            function_name = "Create_GFS_Reference",
            role = role,
            code = _lambda.DockerImageCode.from_image_asset(directory = "create_reference_lambda/"),
            timeout = Duration.minutes(3),
            memory_size=400
            )

        # Grant the function permission to write to CloudWatch Logs
        lambda_reference.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=["arn:aws:logs:*:*:*"]
            )
        )
        

        # This is the lambda that consolidate the references into a single one
        lambda_consolidate = _lambda.DockerImageFunction(
            scope = self,
            id = "ConsolidateReference",
            function_name = "Consolidate_GFS_References",
            role = role,
            code = _lambda.DockerImageCode.from_image_asset(directory = "consolidate_references_lambda/"),
            timeout = Duration.minutes(6),
            memory_size=1000
            )


        # Grant the function permission to write to CloudWatch Logs
        lambda_consolidate.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=["arn:aws:logs:*:*:*"]
            )
        )

        #this is the lambda that writes the trigger files
        lambda_trigger = PythonFunction(
            self,
            id = "CreateReferenceTriggers",
            runtime=_lambda.Runtime.PYTHON_3_9,
            index = "create_trigger.py",
            handler = "handler",
            entry = "trigger_create_lambda/",
            role = role,
            timeout = Duration.minutes(10),
            memory_size=200
        )
        # type: ignore

        # Grant the function permission to write to CloudWatch Logs
        lambda_trigger.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=["arn:aws:logs:*:*:*"]
            )
        )
        
        bucket = s3.Bucket(self, "GFS_Reference")
        bucket.grant_read_write(lambda_reference)
        bucket.grant_read_write(lambda_trigger)
        bucket.grant_read_write(lambda_consolidate)

        #create bucket trigger
        lambda_reference.add_event_source(S3EventSource(bucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="triggers/")]
        ))
