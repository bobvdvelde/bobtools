import logging
import multiprocessing

import cloudpickle


class _MultiWorker(multiprocessing.Process):
    def __init__(self, task_queue, results_queue, func):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.results_queue = results_queue
        self.func = cloudpickle.loads(func)
        logging.info(f"{self.name} : Started")

    def run(self):
        proc_name = self.name
        while True:
            task = self.task_queue.get()
            if task is None:
                logging.info(f"{proc_name} is exiting")
                self.task_queue.task_done()
                break
            logging.info(
                f"{proc_name}-{self.func.__name__} called on {task} (type: {type(task)})"
            )
            if type(task) == dict:
                result = self.func(**task)
            elif type(task) == tuple:
                result = self.func(*task)
            else:
                result = self.func(task)
            self.results_queue.put(result)
            self.task_queue.task_done()
        return


class _SingleWorker(multiprocessing.Process):
    def __init__(self, task_queue, result_queue, func):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.func = cloudpickle.loads(func) if func else None
        logging.info(f"{self.name} : Started")

    def run(self):
        while True:
            task = self.task_queue.get()
            if task is None:
                logging.info(f"{self.name} is exiting")
                self.task_queue.task_done()
                self.result_queue.put(None)
                break
            logging.info(f"{self.name} called on {task}")

            if not self.func:
                self.result_queue.put(task)
                self.task_queue.task_done()
                continue

            elif type(task) == dict:
                result = self.func(**task)
            elif type(task) == tuple:
                result = self.func(*task)
            else:
                result = self.func(task)
            # if the function returns a result, put it in the output_queue
            if not isinstance(result, type(None)):
                self.result_queue.put(result)
            self.task_queue.task_done()
        return


def funnel(
    iterable,
    multi_func,
    single_func=None,
    n_workers=3,
    concurrent_tasks=(100, 100, 100),
    timeout=None,
):
    """Fan-in multiprocessing utility function

    The 'funnel' generator is meant for 'fan-in' multiprocessing, where an
    iterable of data is processed in parallel by `n_workers` and passed to
    a single-thread post-processing worker. For example: Downloading many
    sites in parallel and writing them to a single file.

    Parameters
    ---
    iterable : Iterable
        An iterable of data, such as a list or generator. Items in the iterable
        are individually pickled and passed to the workers to apply the `multi_func`
        stage

    multi_func : Callable
        A function or method that can be applied to any item from the iterable. NOTE:
        a tuple passed to the multi-func will be considered as positional
        arguments (*args), a dictionary passed to the multi func will be considered
        keyword arguments (**kwargs).  Avoid unwanted unpacking by passing
        dictionaries or tuples that should not be interpreted
        as arguments wrapped in a tuple, e.g. ({"my_dict":"is not a set of kwargs},)

    single_func : Callable (default None)
        A function that should NOT process the results of the multi_func in parallel,
        but individually in the same thread. Will simply pass through `multi_func`
        results when set to None.
        NOTE: a tuple passed BY the multi-func will be considered as positional
        arguments (*args), a dictionary passed BY the multi func will be considered
        keyword arguments (**kwargs).  Avoid unwanted unpacking by passing
        dictionaries or tuples that should not be interpreted
        as arguments wrapped in a tuple, e.g. ({"my_dict":"is not a set of kwargs},)
    timeout : int
        How long to wait for jobs to finish
    """
    logging.warning(
        "Funnel is EXPERIMENTAL and is not assured to run correctly in any environment."
        " DO NOT USE IN PRODUCTION"
    )

    # Sanity check
    if n_workers < 2:
        raise ValueError(
            "`n_workers` should be 2 or higher; "
            "At least 1 multi-worker and 1 single-worker is needed."
        )

    # set up queues
    multi_func_queue = multiprocessing.JoinableQueue(maxsize=concurrent_tasks[0])
    single_func_queue = multiprocessing.JoinableQueue(maxsize=concurrent_tasks[1])
    output_queue = multiprocessing.Queue(maxsize=concurrent_tasks[2])

    # set up workers
    pmfunc = cloudpickle.dumps(multi_func)
    psfunc = cloudpickle.dumps(single_func)

    multi_workers = [
        _MultiWorker(multi_func_queue, single_func_queue, pmfunc)
        for i in range(n_workers - 1)
    ]
    single_worker = _SingleWorker(single_func_queue, output_queue, psfunc)

    for w in multi_workers:
        w.start()
    single_worker.start()

    try:

        # Start processing
        for i in iterable:
            if not output_queue.empty():
                yield output_queue.get_nowait()
            multi_func_queue.put(i)

        for i in range(n_workers - 1):
            multi_func_queue.put(None)

        multi_func_queue.join()
        single_func_queue.put(None)
        single_func_queue.join()

        while True:
            result = output_queue.get(timeout=30)
            if isinstance(result, type(None)):
                break
            yield result

    finally:

        # cleanup
        for w in multi_workers:
            w.terminate()
        single_worker.terminate()
        multi_func_queue.close()
        single_func_queue.close()
        output_queue.close()
