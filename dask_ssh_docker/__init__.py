import copy
import logging
import os
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


async def get_docker_image(connection, image):
    """Some images in e.g. private repositories require use of the "docker
    login" command before an image may be pulled.  However, we don't really
    want to handle the user password.  So instead, grab the auth from the
    current user, if it applies, and clean it up when done.
    """
    import asyncssh
    docker_server = image.split('/', 1)[0]

    import json
    dat = json.load(open(os.path.expanduser('~/.docker/config.json')))
    server_keys = dat.get('auths', {}).get(docker_server)
    if server_keys is None:
        # Nothing to do.
        logger.info('docker_login=True specified, but current user has '
                'no credentials at ' + docker_server)
        return

    # We want to be as non-destructive as possible.  So first verify that
    # the target does not have a credential, and back up whatever credentials
    # they do have.
    old_json = None
    try:
        r = await connection.run('cat ~/.docker/config.json', check=True)
    except asyncssh.ProcessError as e:
        # No such file
        pass
    else:
        old_json = json.loads(r.stdout)

    if (old_json is not None
            and old_json.get('auths', {}).get(docker_server, {}).get('auth')
                == server_keys['auth']):
        # Nothing to do, auths match already
        return

    # OK, need to add this server key, overwrite, pull, and restore file
    if old_json is None:
        new_json = {'auths': {docker_server: server_keys}}
    else:
        new_json = copy.deepcopy(old_json)
        new_json.setdefault('auths', {})[docker_server] = server_keys

    try:
        await connection.run(f'mkdir -p ~/.docker', check=True)
        await connection.run(f'cat > ~/.docker/config.json', check=True,
                input=json.dumps(new_json))
    except asyncssh.ProcessError as e:
        logger.error(f'stderr: {e.stderr}')
        raise e

    try:
        # Pull image
        await connection.run(f'docker pull {image}',
                check=True)
    except asyncssh.ProcessError as e:
        logger.error(f'stderr: {e.stderr}')
        raise e
    finally:
        if old_json is not None:
            await connection.run('cat > ~/.docker/config.json', check=True,
                    input=json.dumps(old_json))
        else:
            await connection.run('rm ~/.docker/config.json', check=True)


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
    image: str
        The docker image containing the required libraries
    docker_login: bool
        If True, specifies that the user executing dask_ssh_docker has Docker
        credentials which should be used to pull images on the remote targets.
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
        docker_login: bool,
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
        self.docker_login = docker_login
        self.kwargs = kwargs
        self.name = name

        super().__init__()

    async def start(self):
        import asyncssh  # import now to avoid adding to module startup time

        address, *opts = self.address.split(' ')

        self.connection = await asyncssh.connect(address, **self.connect_options)
        if self.docker_login:
            await get_docker_image(self.connection, self.image)
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
        tries = 99  # Sometimes a blank line is just a blank line
        while True:
            line = await self.proc.stdout.readline()
            if not line.strip():
                tries -= 1
                if tries != 0:
                    continue
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
            docker_login: bool, kwargs: dict):
        self.address = address
        self.image = image
        self.docker_login = docker_login
        self.kwargs = kwargs
        self.connect_options = connect_options

        super().__init__()

    async def start(self):
        import asyncssh  # import now to avoid adding to module startup time

        logger.debug("Created Scheduler Connection")

        address, *opts = self.address.split(' ')

        self.connection = await asyncssh.connect(address, **self.connect_options)

        if self.docker_login:
            await get_docker_image(self.connection, self.image)
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
        tries = 99  # Sometimes a blank line is just a blank line
        while True:
            line = await self.proc.stdout.readline()
            if not line.strip():
                tries -= 1
                if tries != 0:
                    continue
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
    docker_login: bool = False,
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
    docker_login:
        Some remote repositories, such as when image is specified as
        ``'docker.repository.com:port/imagename:tag'``, require authentication.
        If the ``docker_login`` flag is set, then the current user's
        ~/.docker/config.json will be parsed for the given repository and
        transferred to the remote machine so that it might pull down the
        image.

        This is disabled by default, because transferring auth without opting
        in to that behavior would be concerning.
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
            "docker_login": docker_login,
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
                "docker_login": docker_login,
                "worker_module": worker_module,
                "kwargs": {},
            },
        }
        for i, host in enumerate(hosts[1:])
    }
    return SpecCluster(workers, scheduler, name="SSHCluster", **kwargs)
