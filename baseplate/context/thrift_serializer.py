import kombu
from thrift.util.Serializer import serialize, deserialize
from thrift.protocol.TBinaryProtocol import TBinaryProtocolFactory

class ThriftSerializer(object):
  def __init__(self, TClass):
    self.TClass = TClass

  def serialize(self, body):
    return serialize(TBinaryProtocolFactory(), self.TClass())

  def deserialize(self, body):
    thrift_obj = self.TClass()
    deserialize(TBinaryProtocolFactory, thrift_obj, body)
    return thrift_obj

  def register(self):
    kombu.serialization.register(
        'thrift_serializer_%s' % (self.TClass.__name__),
        self.deserialize,
        self.serialize,
        content_type='application/x-thrift',
        content_encoding='binary',
      )
