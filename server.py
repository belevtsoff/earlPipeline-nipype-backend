from earlpipeline import server
import nipype_wrapper_interfaces as backend

if __name__ == '__main__':
    server.set_backend(backend)
    server.run(54123, address='localhost')
