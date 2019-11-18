# Import custom code stored in docker image
import my_library

import dask

def main(argv=[]):
    print(dask.delayed(my_library.func)(range(100)).compute())


if __name__ == '__main__':
    main()

