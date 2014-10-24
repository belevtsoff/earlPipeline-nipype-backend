import nipype.pipeline.engine as pe
from nipype.interfaces.utility import Function

def Hello():
   import os
   from nipype import logging
   iflogger = logging.getLogger('interface')
   message = "Hello "
   file_name =  'hello.txt'
   iflogger.info(message)
   with open(file_name, 'w') as fp:
       fp.write(message)
   return os.path.abspath(file_name)

def World(in_file='asd'):
   from nipype import logging
   iflogger = logging.getLogger('interface')
   message = "World!"
   iflogger.info(message)
   with open(in_file, 'a') as fp:
       fp.write(message)

hello_iface=Function(input_names=[],
                  output_names=['out_file'],
                  function=Hello)

world_iface=Function(input_names=['in_file'],
                  output_names=[],
                  function=World)

hello = pe.Node(name='hello',
               interface=Function(input_names=[],
                                  output_names=['out_file'],
                                  function=Hello))
world = pe.Node(name='world',
               interface=Function(input_names=['in_file'],
                                  output_names=[],
                                  function=World))

pipeline = pe.Workflow(name='nipype_demo')
pipeline.add_nodes([hello, world])
#pipeline.connect([(hello, world, [('out_file', 'in_file')])])
#pipeline.run()
#pipeline.write_graph(graph2use='flat')
