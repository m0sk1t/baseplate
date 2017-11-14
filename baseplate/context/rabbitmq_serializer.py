import kombu
from thrift.util.Serializer import serialize, deserialize

class ThriftSerializer(object):
  def __init__(self, TClass):
    self.TClass = TClass

  def serialize(self, body):
    return serialize(self.TClass(body))

  def deserialize(self, body):
    t_class = self.TClass()
    deserialize(t_class, body)
    return t_class

  def register(self):
    kombu.serialization.register(
        'thrift_serializer_%s' % (self.TClass.__name__),
        self.deserialize,
        self.serialize,
        content_type='application/x-thrift',
        content_encoding='binary',
      )
