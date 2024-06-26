from marshmallow import Schema, fields, validate

class RequestsQuerySchema(Schema):
    dataset = fields.Str(required=True)

class RequestsPostData(Schema):
    dataset = fields.Str(required=True)
    scope = fields.List(fields.Str())
    testing = fields.Boolean()

class SubscribePostData(Schema):
    dataset = fields.Str(required=True)
    scope = fields.List(fields.Str())
    subscription_data = fields.Str(data_key='subscriptionData', required=True)
    notify_url = fields.Str(data_key='notifyURL', required=True)

class MockNotifyPostData(Schema):
    subscription_data = fields.Str(data_key='subscriptionData', required=True)

class AuthorizePostData(Schema):
    dataset = fields.Str(required=True)
    package = fields.Str()
    filename = fields.Str(data_key='file')

class DownloadQuerySchema(Schema):
    token = fields.Str(required=True)
