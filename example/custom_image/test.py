import dask.distributed
import dask_ssh_docker

cluster = dask_ssh_docker.SSHDockerCluster(
        ['localhost', 'localhost --nprocs 2'],
        image='dask-ssh-docker-test:latest')
client = dask.distributed.Client(cluster)

import my_library
print(dask.delayed(my_library.func)(range(100)).compute())

