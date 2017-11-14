import sys
import kombu

from ...context.thrift_serializer import ThriftSerializer

from ... import config


class MessageContext(object):
    pass


class BaseplateConsumer(kombu.Consumer):
    """kombu's Consumer extension for baseplate.

    :param kombu.ChannelT channel: The connection/channel to use for this
        consumer.
    :param baseplate.core.Baseplate baseplate: The baseplate instance for your
        application.

    """
    def __init__(self, channel, baseplate, **kwargs):
        self.baseplate = baseplate
        super(BaseplateConsumer, self).__init__(channel, **kwargs)

    def receive(self, body, message):
        context = MessageContext()
        context.trace = self.baseplate.make_server_span(
            context,
            # TODO: Name needs to be passed from outside?
            name="TODO",
            # TODO: trace_info
            # trace_info=trace_info,
        )

        # TODO: Add message's headers to trace
        context.trace.start()

        try:
            for callback in self.callbacks:
                callback(body, message, context)
        except:
            context.trace.finish(exc_info=sys.exc_info())
            raise
        else:
            context.trace.finish()


class BaseplateConsumerFactory(object):
    """
    Consumer factory class for injecting dependencies into BaseplateConsumer
        during application construction.
    """
    def __init__(self, handler, baseplate, thrift_class=None):
        self.handler = handler
        self.__callbacks = handler.get_callbacks()
        assert self.__callbacks, "At least one callback must be specified"
        self.baseplate = baseplate
        if thrift_class is not None:
            serializer = ThriftSerializer(thrift_class)
            serializer.register()
            self.serializer = serializer

    def get_consumers(self, channel, queues):
        consumer = BaseplateConsumer(
            channel,
            self.baseplate,
            queues=queues,
            callbacks=self.__callbacks,
            accept=['thrift_serializer_%s' % (self.serializer.TClass.__name__)] if self.serializer else None
            )

        return [consumer]


def queue_from_config(app_config, prefix="rabbit."):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    # TODO: other config parameters
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "queue_name": config.String,
            "queue_durable": config.Optional(config.Boolean, False),
            "queue_exclusive": config.Optional(config.Boolean, False),
        },
    })

    options = getattr(cfg, config_prefix)
    # get all prefs, prefixed with queue_ and build Queue kwargs
    remapped_opts = [(k.replace("queue_", ""), v)
                     for k, v in options.items()
                     if k.startswith("queue")]

    options = dict(remapped_opts)
    return kombu.Queue(**options)


def exchange_from_config(app_config, prefix="rabbit."):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "exchange_name": config.String,
            # TODO: enum?
            "exchange_type": config.String,
        },
    })

    options = getattr(cfg, config_prefix)
    return kombu.Exchange(**options)


def connection_from_config(app_config, prefix="rabbit."):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            # TODO: config.Endpoint?
            "connection_url": config.String,
        },
    })

    url = getattr(cfg, config_prefix).connection_url

    return kombu.Connection(url)
