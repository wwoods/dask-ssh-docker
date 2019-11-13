import logging
import sys
from typing import List
import warnings
import weakref

import dask

from distributed.deploy.spec import SpecCluster, ProcessInterface
from distributed.scheduler import Scheduler as _Scheduler
from distributed.worker import Worker as _Worker
from distributed.utils import serialize_for_cli

logger = logging.getLogger(__name__)

def get_docker_cmd(image, global_config):
    """Returns a docker command which accepts additional arguments as the
    command to run within the image.
    """
    return [
            'docker',
            'run',
            '--net=host',
            '--rm',
            '-i',
            '--env', 'DASK_INTERNAL_INHERIT_CONFIG="%s"' % serialize_for_cli(global_config),
            image,
    ]


def get_docker_createprocess_kwargs():
    return dict(
            # Important for docker container to auto-abort...
            term_type='ansi',
            # Though it means that stderr gets merged into stdout.
    )


class Process(ProcessInterface):
    """ A superclass for SSH Workers and Nannies

    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(self, **kwargs):
        self.connection = None
        self.proc = None
        super().__init__(**kwargs)

    async def start(self):
        assert self.connection
        weakref.finalize(
            self, self._kill
        )  # https://github.com/ronf/asyncssh/issues/112
        await super().start()

    async def close(self):
        self._kill()  # self.proc.kill()  # https://github.com/ronf/asyncssh/issues/112
        self.connection.close()
        await super().close()

    def _kill(self):
        """Proper kill, invoking ctrl+c as per asyncssh/issues/112.
        """
        import asyncssh
        try:
            self.proc.stdin.write('\x03')
        except BrokenPipeError:
            pass

        # Follow up with the more traditional form.
        self.proc.kill()

    def __repr__(self):
        return "<SSH %s: status=%s>" % (type(self).__name__, self.status)


class Worker(Process):
    """ A Remote Dask Worker controled by SSH

    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    address: str
        The hostname where we should run this worker
    worker_module: str
        The python module to run to start the worker.
    connect_options: dict
        kwargs to be passed to asyncssh connections
    kwargs: dict
        These will be passed through the dask-worker CLI to the
        dask.distributed.Worker class
    """

    def __init__(
        self,
        scheduler: str,
        address: str,
        connect_options: dict,
        image: str,
        kwargs: dict,
        worker_module="distributed.cli.dask_worker",
        loop=None,
        name=None,
    ):
        # Initially a space-delimited list of options.
        self.address = address
        self.scheduler = scheduler
        self.worker_module = worker_module
        self.connect_options = connect_options
        self.image = image
        self.kwargs = kwargs
        self.name = name

        super().__init__()

    async def start(self):
        import asyncssh  # import now to avoid adding to module startup time

        address, *opts = self.address.split(' ')

        self.connection = await asyncssh.connect(address, **self.connect_options)
        self.proc = await self.connection.create_process(
                " ".join(
                    get_docker_cmd(self.image, dask.config.global_config) + [
                        "python",
                        "-m",
                        self.worker_module,
                        self.scheduler,
                        "--name",
                        str(self.name),
                    ]
                    # Note that opts gets re-joined with ' ', making it
                    # verbatim.
                    + opts
                ),
                **get_docker_createprocess_kwargs(),
        )

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stdout.readline()
            if not line.strip():
                raise Exception("Worker failed to start")
            logger.info(line.strip())
            if "worker at" in line:
                self.address = line.split("worker at:")[1].strip()
                self.status = "running"
                break
        logger.debug("%s", line)
        await super().start()


class Scheduler(Process):
    """ A Remote Dask Scheduler controlled by SSH

    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    connect_options: dict
        kwargs to be passed to asyncssh connections
    kwargs: dict
        These will be passed through the dask-scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(self, address: str, connect_options: dict, image: str,
            kwargs: dict):
        self.address = address
        self.image = image
        self.kwargs = kwargs
        self.connect_options = connect_options

        super().__init__()

    async def start(self):
        import asyncssh  # import now to avoid adding to module startup time

        logger.debug("Created Scheduler Connection")

        address, *opts = self.address.split(' ')

        self.connection = await asyncssh.connect(address, **self.connect_options)

        worker_cmd = " ".join(
                get_docker_cmd(self.image, dask.config.global_config) + [
                    "python",
                    "-m",
                    "distributed.cli.dask_scheduler",
                ]
                + opts
        )
        self.proc = await self.connection.create_process(worker_cmd,
                **get_docker_createprocess_kwargs())

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stdout.readline()
            if not line.strip():
                raise Exception("Worker failed to start: " + worker_cmd)
            logger.info(line.strip())
            if "Scheduler at" in line:
                self.address = line.split("Scheduler at:")[1].strip()
                break
        logger.debug("%s", line)
        await super().start()


def SSHDockerCluster(
    hosts: List[str] = ['localhost', 'localhost'],
    image: str = 'daskdev/dask:latest',
    connect_options: dict = {},
    worker_module: str = "distributed.cli.dask_worker",
    **kwargs
):
    """ Deploy a Dask cluster using SSH

    The SSHCluster function deploys a Dask Scheduler and Workers for you on a
    set of machine addresses that you provide.  The first address will be used
    for the scheduler while the rest will be used for the workers (feel free to
    repeat the first hostname if you want to have the scheduler and worker
    co-habitate one machine.)

    You may configure the scheduler and workers by passing space-delimited
    arguments for ``dask-scheduler`` and ``dask-worker`` after the address
    in the ``hosts`` list.

    You may configure your use of SSH itself using the ``connect_options``
    keyword, which passes values to the ``asyncssh.connect`` function.  For
    more information on these see the documentation for the ``asyncssh``
    library https://asyncssh.readthedocs.io .

    Parameters
    ----------
    hosts: List[str]
        List of hostnames or addresses on which to launch our cluster
        The first will be used for the scheduler and the rest for workers

        Note that these are formatted as 'localhost [--scheduler-opt value]...'
        and 'localhost [--worker-opt value]' - that is, the address followed
        by a space-delimited list of arguments to send to dask-scheduler
        and dask-worker, respectively.
    connect_options:
        Keywords to pass through to asyncssh.connect
        known_hosts: List[str] or None
            The list of keys which will be used to validate the server host
            key presented during the SSH handshake.  If this is not specified,
            the keys will be looked up in the file .ssh/known_hosts.  If this
            is explicitly set to None, server host key validation will be disabled.
    worker_module:
        Python module to call to start the worker

    Examples
    --------
    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "localhost", "localhost", "localhost"],
    ...     connect_options={"known_hosts": None},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"}
    ... )
    >>> client = Client(cluster)

    An example using a different worker module, in particular the
    ``dask-cuda-worker`` command from the ``dask-cuda`` project.

    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "hostwithgpus", "anothergpuhost"],
    ...     connect_options={"known_hosts": None},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"},
    ...     worker_module='dask_cuda.dask_cuda_worker')
    >>> client = Client(cluster)

    See Also
    --------
    dask.distributed.Scheduler
    dask.distributed.Worker
    asyncssh.connect
    """

    assert isinstance(hosts, list), hosts
    assert isinstance(image, str), image

    scheduler = {
        "cls": Scheduler,
        "options": {
            "address": hosts[0],
            "connect_options": connect_options,
            "image": image,
            "kwargs": {},
        },
    }
    workers = {
        i: {
            "cls": Worker,
            "options": {
                "address": host,
                "connect_options": connect_options,
                "image": image,
                "worker_module": worker_module,
                "kwargs": {},
            },
        }
        for i, host in enumerate(hosts[1:])
    }
    return SpecCluster(workers, scheduler, name="SSHCluster", **kwargs)