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
            code = _lambda.DockerImageCode.from_image_asset(directory = "lambda/"),
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
        

        #this is the lambda that writes the trigger files
        lambda_trigger = PythonFunction(
            self,
            id = "CreateReferenceTriggers",
            runtime=_lambda.Runtime.PYTHON_3_9,
            index = "create_trigger.py",
            handler = "handler",
            entry = "trigger/",
            role = role,
            timeout = Duration.minutes(10),
            memory_size=200
        )

        # Grant the function permission to write to CloudWatch Logs
        lambda_trigger.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=["arn:aws:logs:*:*:*"]
            )
        )
        
        # create a CloudWatch Event rule
        #0z 
        #cron_rule_0z = events.Rule(self, "cron_trigger_0z",
        #    schedule=events.Schedule.cron(
        #        minute='0',
        #        hour='6',
        #        month='*',
        #        week_day='*',
        #        year='*'
        #    ),
        #)

        #12z 
        #cron_rule_12z = events.Rule(self, "cron_trigger_12z",
        #    schedule=events.Schedule.cron(
        #        minute='0',
        #        hour='16',
        #        month='*',
        #        week_day='*',
        #        year='*'
        #    ),
        #)
        
        # add the lambda function as a target for the rule
        #cron_rule_0z.add_target(lambda_trigger)    
        #cron_rule_12z.add_target(lambda_trigger)

        bucket = s3.Bucket(self, "GFS_Reference")
        bucket.grant_read_write(lambda_reference)
        bucket.grant_read_write(lambda_trigger)

        #create bucket trigger
        lambda_reference.add_event_source(S3EventSource(bucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="triggers/")]
        ))
