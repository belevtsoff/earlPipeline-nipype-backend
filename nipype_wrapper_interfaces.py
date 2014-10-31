"""A set of nipype's Function interface implementations for machine learning
applications and corresponding earlPipeline wrappers for them"""

# Nipype wrapper base classes
from nipype_wrapper_base import NipypeWrapperUnit as Unit
from nipype_wrapper_base import NipypeWrapperPipeline as Pipeline

# Parameter descriptor
from earlpipeline.backends.base import Parameter

# Nipype API
from nipype.interfaces.utility import Function
from nipype.pipeline.engine import Workflow, Node, MapNode

# common import strings
COMMON_IMPORTS = ['import logging', 'import time', 'import os', 'import numpy as np']
MVPA_IMPORTS = ['from mvpa2 import suite']
NBACK_IMPORTS = ['import util', 'from nback_io import get_data']
IMPORTS = COMMON_IMPORTS + MVPA_IMPORTS + NBACK_IMPORTS

# parameter shortcuts
def text_parameter(name, default):
    return Parameter(name, 'input', str, default, datatype='text')
def float_parameter(name, default):
    return Parameter(name, 'input', float, default, datatype='number')

# ---------------------------------------------------------------------------
# Range Source
# ---------------------------------------------------------------------------

def generate_data(N):
    return range(N)

range_iface = Function(input_names=['N'],
                 output_names=['list'],
                 function = generate_data)

class RangeSource(Unit):
    interface = range_iface
    tag = 'Sources'

    N = Parameter(name='N',
            parameter_type='input',
            value_type=int,
            default_value=10,
            datatype='number')

# ---------------------------------------------------------------------------
# Nback Source
# ---------------------------------------------------------------------------

def reader_func(logger_name, data_path, condition, subject):
    logger = logging.getLogger(logger_name)
    logger.info("Loading data...")

    ds = get_data(condition, subject, data_path)

    ds.a.imghdr.set_data_dtype(np.dtype('int16'))
    ds.sa['chunks_balanced'] = ds.chunks % 4

    return ds

reader_iface = Function(input_names=['logger_name', 'data_path', 'condition', 'subject'],
        output_names=['data'],
        function=reader_func,
        imports=IMPORTS)

class NbackSource(Unit):
    interface = reader_iface
    tag = 'Sources'

    data_path = text_parameter('data_path', '/mnt/antares_raid/groups/norway/Nback/nipype_wrapper/data')
    condition = text_parameter('condition', 'Bipolar')
    subject = text_parameter('subject', '1000')



# ---------------------------------------------------------------------------
# PplMapper
# ---------------------------------------------------------------------------

# This is a special node which allows to run a whole pipeline over a set of
# parameters. It contains a fixed number of input and output ports that can be
# mapped to inputs and outputs of the pipeline via corresponding node
# parameters.

#N_in=5
#N_out=5
#in_port_pat="in%s"
#out_port_pat="out%s"
#in_map_pat="in_map%s"
#out_map_pat="out_map%s"
#tmp_file_pat="tmp_out_%s"

#in_ports = [in_port_pat % i for i in range(N_in)]
#out_ports = [out_port_pat % i for i in range(N_out)]
#in_maps = [in_map_pat % i for i in range(N_in)]
#out_maps = [out_map_pat % i for i in range(N_out)]

#def run_pipeline(**kwargs):
    #"""Builds and runs a pipeline."""
    #import pickle
    #from nipype.pipelines.engine import Workflow, Node
    #from nipype.interfaces.utility import Function
    #from nipype_wrapper_base import Pipeline

    ##import ipdb; ipdb.set_trace()

    #wf = Workflow()

    ## store the output of the pipeline temporarily
    #def write_tmp_out(i, data):
        #import pickle
        #with open(tmp_file_pat % i, 'w') as f:
            #pickle.dump(data, f)
    #tmp_iface = Function(input_names=['i', 'data'],
                         #output_names=[],
                         #function=write_tmp_out)

    #wf_node = Pipeline.load(kwargs['pipeline_file'])._workflow
    #wf.add_nodes([wf_node])

    ## strange bugfix for a strange bug
    #getattr(wf_node, 'inputs')
    #getattr(wf_node, 'outpts')
    
    ## set the values of the input port to the port of the loaded pipeline
    ## according to specified maps
    #for idx, imap in enumerate(in_maps):
        #node, port = imap.split(".")
        #if imap != 'unset':
            #getattr(wf_node.inputs, node).set(**{port:kwargs[in_port_pat % i]})

    #for idx, omap in enumerate(out_maps):
        #node_name, port = omap.split(".")
        #if omap != 'unset':
            #tmp_node = Node(tmp_iface, 'tmp_%s'%idx)
            #tmp_node.inputs.i = idx
            #wf.add_nodes([tmp_node])
            #wf.connect(wf_node, port, tmp_node, 'data')

    #wf.run()

    #outputs = []
    #for idx, omap in enumerate(out_maps):
        #node, port = omap.split(".")
        #if omap != 'unset':
            #with open(tmp_file_pat % idx) as f:
                #data = pickle.load(f)
            #outputs.append(data)
        #else:
            #outputs.append(None)

    #return outputs

#def foo(**kwargs):
    #import logging
    #logger = logging.getLogger('asdfg')
    #logger.info(kwargs['in_map2'])
    #return range(5)

#iface = Function(input_names=in_ports+in_maps+out_maps,
                 #output_names=out_ports,
                 #function=foo)

#in_params = [(name, {'name':name, 'parameter_type': 'text', 'value_type':str, 'default_value':'unset'}) for name in in_maps]
#out_params = [(name, {'name':name, 'parameter_type': 'text', 'value_type':str, 'default_value':'unset'}) for name in out_maps]

#PipelineMapper = create_nipype_unit_class('PplMapper', iface, tag="Utility",
        #node_type=MapNode,
        #parameters=in_params+out_params,
        #node_attrs={'iterfield': in_ports})


# -----------------------------------------------------------------------------
# DataLogger
# -----------------------------------------------------------------------------

def data_logger_func(logger_name, data):
    import logging
    logger = logging.getLogger(logger_name)
    logger.info(str(data))

data_logger_iface = Function(input_names=['logger_name', 'data'],
                 output_names=[],
                 function = data_logger_func)

class DataLogger(Unit):
    interface=data_logger_iface
    tag='Sinks'

    logger_name = Parameter(name='logger_name',
            parameter_type='text',
            value_type=str,
            default_value='data_logger')

#DataLogger = create_nipype_unit_class('DataLogger', iface, tag="Sinks")

#DataLogger = create_nipype_unit_class('DataLogger', iface, tag="Sinks",
        #parameters = [('logger_name', {
            #'name': 'logger_name',
            #'parameter_type': 'text',
            #'value_type': str,
            #'default_value': 'data_logger',
            #})])


def get_unit_types():
    #return [DataSource, PipelineMapper, DataLogger]
    return [RangeSource, NbackSource, DataLogger]

