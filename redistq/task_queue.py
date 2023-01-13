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

    def __init__(self, host, name, reset=False, ttl_zero_callback=None):
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
            retry_on_timeout=True,
        )

    def __len__(self):
        return (self.conn.llen(self._queue)
                + self.conn.llen(self._processing_queue))

    def add(self, task, lease_timeout, ttl=3):
        """Add a task to the task queue.

        Parameters
        ----------
        task : something that can be JSON-serialized
        lease_timeout : float
            lease timeout in seconds, i.e. how much time we give the
            task to process until we can assume it didn't succeed
        ttl : int
            Number of (re-)tries, including the initial one, in case the
            job dies.

        """
        # make sure the timeout is an actual number, otherwise we'll run
        # into problems later when we calculate the actual deadline
        lease_timeout = float(lease_timeout)

        # we wrap the task itself with some meta data
        id_ = uuid4().hex
        wrapped_task = {
            'ttl': ttl,
            'task': task,
            'lease_timeout': lease_timeout,
            'deadline': None,
        }
        task = self._serialize(wrapped_task)
        # store the task + metadata and
        # put task-id into the task queue
        # uses pipeline for network optimization(reduces RTT time)
        with self.conn.pipeline() as pipeline:
            pipeline.multi()
            pipeline.set(self._tasks + id_, task)
            pipeline.lpush(self._queue, id_)
            pipeline.execute()

    def get(self):
        """Get a task from the task queue (non-blocking).

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

        Note, this method is non-blocking, i.e. it returns immediately
        even if there is nothing to return. See below for the return
        value for this case.

        Returns
        -------
        (task, task_id) :
            The next item from the task list or (None, None) if it's
            empty

        """
        # See comment in __init__ about moving jobs between queues in an
        # atomic fashion before you modify!
        while True:
            task_id = self.conn.lindex(self._queue, -1)

            # returns none when queue is empty
            if task_id is None:
                return None, None

            task_id = task_id.decode()
            logger.info(f'Got task with id {task_id}')

            task = self.conn.get(self._tasks + task_id)
            if task is None:
                logger.info(f'{task_id} was marked complete by other worker')
                continue

            task = self._deserialize(task)
            now = time.time()

            with self.conn.pipeline() as pipeline:
                try:
                    # optimistic locking, to avoid race condition and retry.
                    # requires transaction, setting deadline and moving task
                    # from one queue to another should be atomic. If not then
                    # there is a possibility that task do not have any
                    # deadline(None) but it is in the self._processing_queue
                    # and leads to dangling task sitting in redis until
                    # manually removed.
                    pipeline.watch(self._processing_queue,
                                   self._tasks + task_id)
                    task['deadline'] = now + task['lease_timeout']
                    pipeline.multi()    # starts transaction
                    pipeline.set(self._tasks + task_id, self._serialize(task))
                    pipeline.lrem(self._queue, -1, task_id)
                    pipeline.lpush(self._processing_queue, task_id)
                    pipeline.execute()  # ends transaction
                    return task['task'], task_id
                except redis.WatchError:
                    logger.info(f'{task_id} is being processed by another '
                                'worker, will fetch new task')

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
        with self.conn.pipeline() as pipeline:
            pipeline.multi()
            pipeline.lrem(self._processing_queue, 0, task_id)
            # this happens because another worker marked it as expired
            # so it was put back into the queue
            # now we remove it to avoid a futile re-processing
            pipeline.lrem(self._queue, 0, task_id)
            pipeline.delete(self._tasks + task_id)
            pipeline.execute()

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
                    found = self.conn.lpos(self._processing_queue, task_id)
                    # if not found then it returns none, otherwise index
                    if found is None:
                        raise ValueError(f'Task {task_id} does not exist.')
                    task = self.conn.get(self._tasks + task_id)
                    pipeline.watch(self._processing_queue,
                                   self._tasks + task_id)
                    pipeline.multi()
                    pipeline.lrem(self._processing_queue, 0, task_id)
                    pipeline.lpush(self._queue, task_id)

                    task = self._deserialize(task)
                    task['deadline'] = None
                    task = self._serialize(task)
                    pipeline.set(self._tasks + task_id, task)
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

        Note: lease check is only performed against the task in
        self._processing queue, self._queue is untouched since its
        yet to be processed.

        """
        # goes through all the tasks that we currently have in
        # self.processing_queue and check the ones with expired timeout
        now = time.time()
        for task_id in self.conn.lrange(self._processing_queue, 0, -1):

            task_id = task_id.decode()
            task = self.conn.get(self._tasks + task_id)

            if task is None:
                # race condition! between the time we got `key` from the
                # set of tasks (this outer loop) and the time we tried
                # to get that task from the queue, it has been completed
                # and therefore deleted from all queues. In this case
                # tasks is None and we can continue
                logger.info(f"Task {task_id} was marked completed while we "
                            "checked for expired leases, nothing to do.")
                continue

            task = self._deserialize(task)

            if task['deadline'] is None:
                # race condition! task was already moved by other worker
                logger.info(f"Task {task_id} was moved by other worker")
                continue

            if task['deadline'] <= now:
                logger.warning('A lease expired, probably job failed: '
                               f'{task_id} => {task}')
                task['ttl'] -= 1
                # See comment in __init__ about moving jobs between
                # queues in an atomic fashion before you modify!
                with self.conn.pipeline() as pipeline:
                    try:
                        pipeline.watch(self._tasks + task_id)
                        pipeline.multi()
                        if task['ttl'] <= 0:
                            logger.error(f'Job {task} with id {task_id} '
                                         'failed too many times, dropping it.')
                            pipeline.lrem(self._processing_queue, 0, task_id)
                            pipeline.delete(self._tasks + task_id)
                            if self.ttl_zero_callback:
                                self.ttl_zero_callback(task_id, task)
                        else:
                            task['deadline'] = None
                            task = self._serialize(task)
                            pipeline.set(self._tasks + task_id, task)
                            pipeline.lrem(self._processing_queue, 0, task_id)
                            pipeline.lpush(self._queue, task_id)
                        pipeline.execute()
                    except redis.WatchError:
                        logger.info('Skipping lease check, already performed '
                                    'by another worker')

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
