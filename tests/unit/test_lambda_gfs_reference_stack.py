import aws_cdk as core
import aws_cdk.assertions as assertions

from lambda_gfs_reference.lambda_gfs_reference_stack import LambdaGfsReferenceStack

# example tests. To run these tests, uncomment this file along with the example
# resource in lambda_gfs_reference/lambda_gfs_reference_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = LambdaGfsReferenceStack(app, "lambda-gfs-reference")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
