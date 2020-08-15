#!/usr/bin/env python
# -*- coding: utf-8 -*-
from itemadapter import ItemAdapter
from scrapy.utils.misc import load_object
from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet.threads import deferToThread

from scrapy_distributed.amqp_utils import connection, defaults


default_serialize = ScrapyJSONEncoder().encode


class RabbitPipeline(object):
    """Pushes serialized item into a redis list/queue
    Settings
    --------
    AMQP_PIPELINE_QUEUE : str
        amqp queue where to store items.
    AMQP_PIPELINE_SERIALIZER : str
        Object path to serializer function.
    """

    def __init__(
        self,
        channel,
        queue_name=defaults.AMQP_PIPELINE_QUEUE,
        serialize_func=default_serialize,
    ):
        """Initialize pipeline.
        Parameters
        ----------
        channel : 
            pika channel instance.
        queue_name : str
            Redis key where to store items.
        serialize_func : callable
            Items serializer function.
        """
        self.server = server
        self.queue_name = queue_name
        self.serialize = serialize_func

    @classmethod
    def from_settings(cls, settings):
        params = {
            "server": connection.from_settings(settings),
        }
        if settings.get("REDIS_ITEMS_KEY"):
            params["key"] = settings["REDIS_ITEMS_KEY"]
        if settings.get("REDIS_ITEMS_SERIALIZER"):
            params["serialize_func"] = load_object(settings["REDIS_ITEMS_SERIALIZER"])

        return cls(**params)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        key = self.item_key(item, spider)
        data = self.serialize(item)
        self.server.rpush(key, data)
        return item

    def item_key(self, item, spider):
        """Returns redis key based on given spider.
        Override this function to use a different key depending on the item
        and/or spider.
        """
        return self.key % {"spider": spider.name}

