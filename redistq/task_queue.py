import json
import logging
import time
from uuid import uuid4

import redis

logger = logging.getLogger(__name__)


class TaskQueue:
    """Reliable task queue.

    This task queue is able to recover failed task, see `TTL` in `add`.

    On the producing side:

    >>> tq = TaskQueue('localhost', 'myqueue')
    >>> for i in range(10):
    ...     tq.add(some task, lease_timeout)

    On the consuming side:

    >>> tq = TaskQueue('localhost', 'myqueue')
    >>> while True:
    ...     task, task_id = tq.get()
    ...     if task is not None:
    ...         # do something
    ...         tq.complete(task_id)
    ...     if tq.is_empty():
    ...         break

    """

    def __init__(
        self, host, name, timeout=0, reset=False, ttl_zero_callback=None,
    ):
        """Initialize the task queue.

        Note: a task has to be at any given time either in the task
        queue or in the processing queue. If a task is moved from one
        queue to the other it has to be in an atomic fashion!

        Parameters
        ----------
        host : str
            hostname of the redis server
        name : str
            name of the task queue
        timeout : int
            timeout in seconds for getting new tasks and waiting for
            producer to generate ones
        reset : bool
            If true, reset existing keys in the DB that have `name` as
            prefix.
        ttl_zero_callback : callable
            a function that is called if a task's ttl <= 0. The callback
            needs to accept two parameters, the task_id and the task.

        """
        self._queue = name + ':queue'
        self._processing_queue = name + ':processing'
        self._tasks = name + ':tasks:'
        self._host = host

        # timeout for getting new tasks + waiting for producer to
        # generate those tasks
        self.timeout = timeout
        # called when ttl <= 0 for a task
        self.ttl_zero_callback = ttl_zero_callback

        self.socket_timeout = 25

        self.connect()

        if reset:
            self._reset()

    def connect(self):
        """Establish a connection to Redis.
        If a connection already exists, it's overwritten.
        """
        # We use health_check_interval to avoid:
        # https://github.com/andymccurdy/redis-py/issues/1186
        self.conn = redis.Redis(
            host=self._host,
            health_check_interval=30,
            socket_timeout=self.socket_timeout,
        )

    def __len__(self):
        return (self.conn.llen(self._queue)
                + self.conn.llen(self._processing_queue))

    def add(self, task, lease_timeout, ttl=3):
        """Add a task to the task queue.

        Parameters
        ----------
        task : something that can be JSON-serialized
        lease_timeout : int
            lease timeout in seconds, i.e. how much time we give the
            task to process until we can assume it didn't succeed
        ttl : int
            Number of (re-)tries, including the initial one, in case the
            job dies.
        """
        # we wrap the task itself with some meta data
        id_ = uuid4().hex
        wrapped_task = {
            'ttl': ttl,
            'task': task,
            'lease_timeout': lease_timeout,
            'deadline': None,
        }
        task = self._serialize(wrapped_task)
        # store the task + metadata
        self.conn.set(self._tasks + id_, task)
        # put task-id into the task queue
        self.conn.lpush(self._queue, id_)

    def get(self):
        """Get a task from the task queue.

        This method returns the next task from the queue. It also puts
        this task into an internal processing queue so we can keep track
        on ongoing tasks. In order to mark that task as done, you have
        to use:

            >>> task, task_id = taskqueue.get()
            >>> # do something
            >>> taskqueue.complete(task_id)

        After some time (i.e. `lease_timeout`) tasks expire and are
        removed from the processing queue and the TTL is decreased by
        one. If TTL is still > 0 the task will be put back into the task
        queue for retry.

        Returns
        -------
        (task, task_id) :
            The next item from the task list or (None, None) if it's
            empty

        """
        # See comment in __init__ about moving jobs between queues in an
        # atomic fashion before you modify!
        # we need to deal with the socket timeout,
        # see https://github.com/andymccurdy/redis-py/issues/1305
        for _ in range(1 + self.timeout // (self.socket_timeout - 5)):
            try:
                task_id = self.conn.brpoplpush(
                    self._queue,
                    self._processing_queue,
                    self.socket_timeout - 5,
                    )
            except redis.exceptions.TimeoutError:
                logger.exception('Redis timeout, will reset the connection')
                self.connect()
                # we consider the task as None in case of timeout
                # to handle it like an operational timneout
                task_id = None
            if task_id is None:
                continue
            else:
                break

        if task_id is None:
            return None, None
        task_id = task_id.decode()
        logger.info(f'Got task with id {task_id}')

        now = time.time()
        task = self._deserialize(self.conn.get(self._tasks + task_id))
        deadline = now + task['lease_timeout']
        task['deadline'] = deadline
        self.conn.set(self._tasks + task_id, self._serialize(task))

        return task['task'], task_id

    def __iter__(self):
        """Iterate over tasks and mark them as complete.

        This allows to easily iterate over the tasks to process them:

            >>> for task in task_queue:
                    execute_task(task)

        it takes care of marking the tasks as done once they are processed
        and checking the emptiness of the queue before leaving.

        Notice that this iterator can wait for a long time waiting for work
        units to appear, depending on the value set as lease_timeout.

        Yields
        -------
        (any, str) :
            A tuple containing the task content and its id
        """
        while True:
            task, id_ = self.get()
            if task is not None:
                yield task, id_
                self.complete(id_)
            if self.is_empty():
                logger.debug(
                    f'{self._queue} and {self._processing_queue} are empty. '
                    'Nothing to process anymore...'
                )
                break

    def complete(self, task_id):
        """Mark a task as completed.

        Marks a task as completed and removes it from the processing
        queue.

        If the job is in the queue, which happens if it took too long
        and it expired, is removed from that too.


        Parameters
        ----------
        task_id : str
            the task ID

        """
        logger.info(f'Marking task {task_id} as completed')
        removed = self.conn.lrem(self._processing_queue, 0, task_id)
        # check if we actually removed something
        if removed == 0:
            logger.warning(f'Task {task_id} was not being processed, '
                           'so cannot mark as completed.')
            # this happens because another worker marked it as expired
            # so it was put back into the queue
            # now we remove it to avoid a futile re-processing
            deleted_retry = self.conn.lrem(self._queue, 0, task_id)
            # if it's not in the queue, this was the last attempt
            # allowed by the TTL
            if deleted_retry == 0:
                logger.warning(f'Task {task_id} was not not in the queue,'
                               ' this was the last attempt and succeeded.')
        # delete the lease as well
        self.conn.delete(self._tasks + task_id)

    def reschedule(self, task_id):
        """Move a task back from the processing- to the task queue.

        Workers can use this method to "drop" a work unit in case of
        eviction.

        This function does not modify the TTL.

        Parameters
        ----------
        task_id : str
            the task ID

        Raises
        ------
        ValueError :
            Task is not being processed, and cannot be re-scheduled

        """
        # See comment in __init__ about moving jobs between queues in an
        # atomic fashion before you modify!
        with self.conn.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(self._processing_queue, self._tasks)
                    procs = pipeline.lrange(self._processing_queue, 0, -1)
                    procs = [p.decode() for p in procs]
                    if task_id not in procs:
                        raise ValueError(f'Task {task_id} does not exist.')
                    task = self.conn.get(self._tasks + task_id)

                    pipeline.multi()
                    pipeline.lrem(self._processing_queue, 0, task_id)
                    pipeline.lpush(self._queue, task_id)

                    task = self._deserialize(task)
                    task['deadline'] = None
                    task = self._serialize(task)
                    self.conn.set(self._tasks + task_id, task)

                    pipeline.execute()
                    break
                except redis.WatchError:
                    # processing queue has been modified since we tried to
                    # execute the atomic block, the task might not be in
                    # there anymore, retry
                    continue

    def is_empty(self):
        """Check if the task queue is empty (todo and processing).

        This method relies on a job being either in the task- or in the
        processing queue at any given time, see comment in __init__.

        Internally, this function also checks the currently processed
        tasks for expiration and teals with TTL and re-scheduling them
        into the task queue.

        Returns
        -------
        bool

        """
        self._check_expired_leases()
        return (self.conn.llen(self._queue) == 0
                and self.conn.llen(self._processing_queue) == 0)

    def _check_expired_leases(self):
        """Check for expired leases.

        This method goes through all tasks that are currently processed
        and checks if their deadline expired. If not we assume the
        worker died. We decrease the TTL and if TTL is still > 0 we
        reschedule the task into the task queue or, if the TTL is
        exhausted, we remove the task completely.

        """
        # to through all tasks that we currently have and check the ones
        # with expired timeout
        now = time.time()
        for key in self.conn.scan_iter(match=self._tasks + '*'):

            key = key.decode()
            id_ = key.split(':')[-1]

            task = self.conn.get(key)
            if task is None:
                # race condition! between the time we got `key` from the
                # set of tasks (this outer loop) and the time we tried
                # to get that task from the queue, it has been completed
                # and therefore deleted from all queues. In this case
                # tasks is None and we can continue
                logger.info(f"Task {key} was marked completed while we "
                            "checked for expired leases, nothing to do.")
                continue
            task = self._deserialize(task)

            if task['deadline'] is None:
                # task hasn't started yet, we don't care
                continue

            if task['deadline'] <= now:
                logger.warning(
                    f'A lease expired, probably job failed: {key} => {task}')
                task['ttl'] -= 1
                if task['ttl'] <= 0:
                    logger.error(f'Job {task} with it {id_ } failed too many'
                                 ' times, dropping it.')
                    self.conn.lrem(self._processing_queue, 0, id_)
                    self.conn.delete(key)
                    if self.ttl_zero_callback:
                        self.ttl_zero_callback(id_, task)
                    continue

                task['deadline'] = None
                task = self._serialize(task)
                # See comment in __init__ about moving jobs between
                # queues in an atomic fashion before you modify!
                with self.conn.pipeline() as pipeline:
                    while True:
                        try:
                            pipeline.watch(key, self._processing_queue)
                            pipeline.multi()
                            pipeline.set(key, task)
                            pipeline.lrem(self._processing_queue, 0, id_)
                            pipeline.lpush(self._queue, id_)
                            pipeline.execute()
                            break
                        except redis.WatchError:
                            continue

    def _serialize(self, task):
        task = json.dumps(task, sort_keys=True)
        return task

    def _deserialize(self, blob):
        task = json.loads(blob)
        return task

    def _reset(self):
        """Delete all keys in the DB prefixed with our name.

        """
        self.conn.delete(self._queue)
        self.conn.delete(self._processing_queue)
        keys = self.conn.keys(f'{self._tasks}*')
        if keys:
            self.conn.delete(*keys)
