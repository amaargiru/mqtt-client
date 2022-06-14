To establish a connection with the Yandex Cloud you must add file named **private_config.json** to this empty directory with contents like this:  
```
{  
  "publish_topic": "example_topic_pub",
  "subscribe_topics": ["example_topic_sub1", ..., "example_topic_subN"]
}  
```

Learn how to create a topic from the [Yandex Cloud documentation](https://cloud.yandex.com/en-ru/docs/iot-core/concepts/topic/).