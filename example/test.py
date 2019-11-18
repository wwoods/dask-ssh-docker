'''Main script: must define a main() function which can take argv.  The
``dask-ssh-docker`` script handles cluster setup and client connections, so
user code does not need to deal with it.

Run as either

1. Locally:

       python example/test.py

2. Cluster:

       dask-ssh-docker localhost localhost -- example/test.py
'''

import argparse
import dask

def main(argv=[]):
    parser = argparse.ArgumentParser()
    args = parser.parse_args(argv)

    print(dask.delayed(sum)(range(1000)).compute())


if __name__ == '__main__':
    main()

