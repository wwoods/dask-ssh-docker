import logging
logging.basicConfig(level=logging.DEBUG)

from dask_ssh_docker import SSHDockerCluster
cluster = SSHDockerCluster(['localhost', 'localhost --nprocs 4 --nthreads 2'])

import dask.distributed
client = dask.distributed.Client(cluster)

print(dask.delayed(sum)(range(1000)).compute())

