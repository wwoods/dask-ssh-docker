`dask-ssh-docker`: A tool for configuring a [Dask](https://dask.org/) cluster whose dependencies are
distributed via [Docker](https://www.docker.com/).  The reason this exists is that `dask-kubernetes`
was a bit tricky to get working correctly with a local development deployment, and many academic users
have access to worker machines over SSH.

Usage:

```python
# test.py file

import dask

def main(argv=[]):
    # The main() function is used as the entry point when script is run
    # directly.
    print(dask.delayed(sum)(range(1000)).compute()
if __name__ == '__main__':
    main()
```

```sh
# Terminal
$ pip install https://github.com/wwoods/dask-ssh-docker

# Scripts can be run locally:
$ python test.py

# Or, without changes, on a cluster:
$ dask-ssh-docker localhost "localhost --nprocs 2" -- test.py

# See "--help" for more information.
$ dask-ssh-docker --help
usage: dask-ssh-docker [-h] [--docker-login] [--image IMAGE] [-v]
                       hosts [hosts ...]

Launch a distributed cluster over SSH, using docker images to distribute
dependencies and code. The first host will run a 'dask-scheduler' process, and
subsequent hosts become 'dask-worker' processes. Note that, unlike 'dask-ssh',
this script additionally accepts a script to run on the cluster (following
'--'), and accepts arguments next to the hostname instead of separately. E.g.:
dask-ssh-docker 'localhost --nprocs 4' 'otherhost --nprocs 8' .

positional arguments:
  hosts           Either a list of host names with arguments, or a single
                  filename which contains such information on each line.

optional arguments:
  -h, --help      show this help message and exit
  --docker-login  Specify to allow 'dask-ssh-docker' to grab credential from
                  ~/.docker/config.json, if remote hosts need to pull
                  specified image from a private repository. (see 'docker
                  login' command). (default: False)
  --image IMAGE   Custom image containing required dependencies and code for
                  computation. (default: daskdev/dask:latest)
  -v, --verbose   Execute logging.basicConfig(level=logging.DEBUG) before
                  attempting connection, to debug connection issues. (default:
                  False)
```

Without `--nprocs` specified, only one remote process will be made.

Usage with `docker login`
-------------------------

If using a private repository, a `docker login` command might be required for
the remote nodes to be able to pull in the desired image.  In these cases,
`dask-ssh-docker` will automatically use the current user's `~/.docker/config.json`
file to grab the auth key and transmit it to the remote machine for pulling,
if and only if `--docker-login` is specified in `dask-ssh-docker`'s arguments.
E.g.:

```sh
$ dask-ssh-docker --image my-repository.com:5000/dask:latest --docker-login host [host...] -- script.py
```

