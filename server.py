from earlpipeline import server
import nipype_wrapper

if __name__ == '__main__':
    server.set_backend(nipype_wrapper)
    server.run(54123, address='localhost')
