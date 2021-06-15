from marshmallow import Schema, fields, validate


class RequestsQuerySchema(Schema):
    dataset = fields.Str(required=True)


class RequestsPostData(Schema):
    dataset = fields.Str(required=True)
    scope = fields.List(fields.Str())


class SubscribePostData(Schema):
    dataset = fields.Str(required=True)
    scope = fields.List(fields.Str())
    subscription_data = fields.Str(data_key="subscriptionData")
    notify_url = fields.Str(data_key="notifyURL", required=True)


class AuthorizePostData(Schema):
    dataset = fields.Str(required=True)
    package = fields.Str()
    filename = fields.Str(data_key="file")


class DownloadQuerySchema(Schema):
    dataset = fields.Str(required=True)
    token = fields.Str()
    package = fields.Str()
    filename = fields.Str(data_key="file")
