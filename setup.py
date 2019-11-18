from distutils.core import setup

setup(
    name='dask-ssh-docker',
    version='0.1dev',
    packages=['dask_ssh_docker'],
    scripts=['bin/dask-ssh-docker'],
)
