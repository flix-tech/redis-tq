## Redis-based Task Queue

Based on [this example][source]:

[source]: http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html

# Run the tests

The tests will check the presence of a Redis instance on localhost, you can use

    docker run --rm -d -p 6379:6379 redis

to get one. Then use `make test`, it will take care of creating an appropriate
virtualenv and use it to run the tests.
