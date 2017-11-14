import kombu
from thrift.util.Serializer import serialize, deserialize

class RabbitThriftSerializer(object):
  """docstring for RabbitThriftSerializer"""
  def __init__(self, TClass):
    self.TClass = TClass

  def rt_serialize(self, body):
    return serialize(self.TClass(body))

  def rt_deserialize(self, body):
    t_class = self.TClass()
    return deserialize(t_class, body)

  def register_kombu_serializer(self):
    kombu.serialization.register(
        'rabbit_thrift_serializer',
        self.rt_deserialize,
        self.rt_serialize,
        content_type='application/x-thrift',
        content_encoding='binary',
      )
