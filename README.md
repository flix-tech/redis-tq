# redis-tq

redis-tq is a [Redis-based][redis] Multi-producer, multi-consumer Queue.
Allows for sharing data between multiple processes or hosts.

Tasks support a "lease time". After that time other workers may consider this
client to have crashed or stalled and pick up the item instead. The number of
retries can be configured as well.

Based on [this example][source] but with many improvements added.

[source]: http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html
[redis]: https://redis.io/


## Installing

```sh
$ pip install redis-tq
```


## How to use

On the producing side, populate the queue with tasks and a respective lease
timeout:

```python
tq = TaskQueue('localhost', 'myqueue')
for i in range(10):
    tq.add(some task, lease_timeout, ttl=3)
```

On the consuming side:

```python
tq = TaskQueue('localhost', 'myqueue')
while True:
    task, task_id = tq.get()
    if task is not None:
        # do something with task and mark it as complete afterwards
        tq.complete(task_id)
    if tq.is_empty():
        break
```

If the consumer crashes (i.e. the task is not marked as completed after
`lease_timeout` seconds), the task will be put back into the task queue. This
rescheduling will happen at most `ttl` times and then the task will be
dropped. A callback can be provided if you want to monitor such cases.


## Running the tests

The tests will check the presence of a Redis instance on localhost, you can
use

    docker run --rm -d -p 6379:6379 redis

to get one. Then use `make test`, it will take care of creating an appropriate
virtualenv and use it to run the tests.
