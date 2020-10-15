from marshmallow import Schema, fields, validate

class RequestsQuerySchema(Schema):
    dataset = fields.Str(required=True)

class RequestsPostData(Schema):
    dataset = fields.Str(required=True)
    scope = fields.List(fields.Str())

class AuthorizePostData(Schema):
    dataset = fields.Str(required=True)
    package = fields.Str()
    filename = fields.Str(data_key='file')

class DownloadQuerySchema(Schema):
    dataset = fields.Str(required=True)
    token = fields.Str()
    package = fields.Str()
    filename = fields.Str(data_key='file')
