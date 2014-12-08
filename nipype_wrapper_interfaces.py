"""Nipype backend classes"""

# Nipype wrapper base classes
from nipype_wrapper_base import NipypeWrapperUnit as Unit
from nipype_wrapper_base import NipypeWrapperPipeline as Pipeline

# Parameter descriptor
from earlpipeline.backends.base import Parameter

# Nipype API
import nipype.interfaces.io as nio
import nipype.interfaces.fsl as fsl
import nipype.interfaces.utility as util
import os
from nipype.workflows.dmri.fsl.dti import create_eddy_correct_pipeline, create_bedpostx_pipeline

import __builtin__

# parameter shortcuts
def text_parameter(name, default):
    return Parameter(name, 'input', str, default, datatype='text')
def path_parameter(name, default):
    return Parameter(name, 'input', os.path.abspath, default, datatype='text')
def float_parameter(name, default):
    return Parameter(name, 'input', float, default, datatype='number')
def int_parameter(name, default):
    return Parameter(name, 'input', int, default, datatype='number')
def boolean_parameter(name, default):
    return Parameter(name, 'boolean', bool, default)


##############################################################################
# PrimitiveSource
##############################################################################

I = util.IdentityInterface(fields=['str_par', 'float_par', 'int_par', 'bool_par'])

class PrimitiveSource(Unit):
    interface = I
    tag = "Sources"
    instance_name_template = "primsrc"

    # hide the in port. Use it as a parameter
    hidden_in_ports = ['str_par',
                    'float_par',
                    'int_par',
                    'bool_par']
    str_par = text_parameter('str_par', 'subj1')
    float_par = float_parameter('float_par', 1.01)
    int_par = int_parameter('int_par', 2)
    bool_par = boolean_parameter('bool_par', True)


##############################################################################
# IterableSource
##############################################################################

I = util.IdentityInterface(fields=['output_val'])

class IterableSource(Unit):
    interface = I
    tag = "Sources"
    instance_name_template = "itsrc"
    check_in_ports = False

    # obviously
    hidden_in_ports = ['output_val',
            'iterable_file',
            'iterable_type']

    iterable_file = path_parameter('iterable_file', 'iterable.txt')
    iterable_type = Parameter('iterable_type', 'dropdown', str, 'str',
            items = ['str', 'int', 'float'])

    # augment the 'set_parameter' method to set the iterable
    def set_parameter(self, name, value):
        # run the original method first
        super(IterableSource, self).set_parameter(name, value)

        # if it is iterable_file parameter, set the iterables
        if name == 'iterable_file':
            item_type = getattr(__builtin__, self.iterable_type)
            with open(value) as f:
                items = [item_type(line.rstrip('\n')) for line in f.readlines()]
                self._node.iterables = ("output_val", items) 


##############################################################################
# Type Converter
##############################################################################

def type_convert(input_val, output_type):
    import __builtin__
    type_func = getattr(__builtin__, output_type)
    output = type_func(input_val)
    return output

func_iface = util.Function(input_names=['input_val', 'output_type'],
        output_names=['output_val'],
        function=type_convert)

class TypeConverter(Unit):
    interface = func_iface
    tag = "Utility"
    instance_name_template = "conv"

    hidden_in_ports = ['output_type',
            'ignore_exception',
            'function_str']

    ignore_exception = boolean_parameter('ignore_exception', False)
    output_type = Parameter('output_type', 'dropdown', str, 'str',
            items = ['str', 'int', 'float'])


##############################################################################
# DTI Data grabber
##############################################################################

info = dict(dwi=[['subject_id', 'data']],
            bvecs=[['subject_id','bvecs']],
            bvals=[['subject_id','bvals']])

datasource_iface = nio.DataGrabber(infields=['subject_id'],
                                               outfields=info.keys())

# currently, earlPipeline doesn't support passing arbitrary python objects as
# parameters, so the complicated fields like "template_args" etc will be
# hardcoded here for now (as defaults for interface):
datasource_iface.inputs.template = "%s/%s"

datasource_iface.inputs.field_template = dict(dwi='%s/%s.nii.gz')
datasource_iface.inputs.template_args = info

class DTIDataSource(Unit):
    interface = datasource_iface
    tag = "Sources"
    instance_name_template = "dtisrc"

    # only leave "subject_id" as an input port
    hidden_in_ports = ['ignore_exception',
                    'raise_on_empty',
                    'sort_filelist',
                    'template_args',
                    'template',
                    'base_directory',
                    'field_template']

    # expose system ports as parameters
    ignore_exception = boolean_parameter('ignore_exception', False)
    raise_on_empty = boolean_parameter('raise_on_empty', True)

    # expose some ports as parameters
    subject_id = text_parameter('subject_id', 'subj1')
    base_directory = path_parameter('base_directory', '../../fsl_course_data/fdt2')
    sort_filelist = boolean_parameter('sort_filelist', True)



##############################################################################
# FSL ROI extraction
##############################################################################

roi_iface = fsl.ExtractROI()

class ROIExtractor(Unit):
    interface = roi_iface
    tag = "Processing"
    instance_name_template = "roi"

    hidden_in_ports = ['terminal_output',
                    'args',
                    'crop_list',
                    'environ',
                    'ignore_exception',
                    'output_type',
                    'roi_file',
                    't_min',
                    't_size',
                    'x_min',
                    'x_size',
                    'y_min',
                    'y_size',
                    'z_min',
                    'z_size']

    ignore_exception = boolean_parameter('ignore_exception', False)
    terminal_output = Parameter('terminal_output', 'dropdown', str, 'stream',
            items = ['stream', 'allatonce', 'file', 'none'])
    args = text_parameter('args', '')
    output_type = Parameter('output_type', 'dropdown', str, 'NIFTI',
            items = ['NIFTI_PAIR', 'NIFTI_PAIR_GZ', 'NIFTI_GZ', 'NIFTI'])

    # these integer parameters are undefined by default. Currently, the
    # properties descriptor necessarily requires a default value. I'll have to
    # implement undefined parameters before adding all these fields. For now,
    # I'll just use tutorial values as default
    t_min = int_parameter('t_min', 0)
    t_size = int_parameter('t_size', 1)



##############################################################################
# FSL Brain Extraction Tool
##############################################################################

bet_iface = fsl.BET()

class BrainExtractor(Unit):
    interface = bet_iface
    tag = "Processing"
    instance_name_template = "bet"

    hidden_in_ports = ['remove_eyes',
                     'no_output',
                     'out_file',
                     'functional',
                     'radius',
                     'threshold',
                     'surfaces',
                     't2_guided',
                     'ignore_exception',
                     'vertical_gradient',
                     'reduce_bias',
                     'args',
                     'padding',
                     'mesh',
                     'robust',
                     'center',
                     'outline',
                     'skull',
                     'mask',
                     'terminal_output',
                     'environ',
                     'output_type']

    # common stuff
    ignore_exception = boolean_parameter('ignore_exception', False)
    terminal_output = Parameter('terminal_output', 'dropdown', str, 'stream',
            items = ['stream', 'allatonce', 'file', 'none'])
    args = text_parameter('args', '')
    output_type = Parameter('output_type', 'dropdown', str, 'NIFTI',
            items = ['NIFTI_PAIR', 'NIFTI_PAIR_GZ', 'NIFTI_GZ', 'NIFTI'])

    # again, exposing only the tutorial options for now
    mask = boolean_parameter('mask', True)
    frac = float_parameter('frac', 0.34)



##############################################################################
# FSL DTI fitting routine
##############################################################################

dti_iface = fsl.DTIFit()

class DTIFitter(Unit):
    interface = dti_iface
    tag = "Processing"
    instance_name_template = "dtifit"

    hidden_in_ports = ['ignore_exception',
                     'min_x',
                     'min_y',
                     'min_z',
                     'args',
                     'terminal_output',
                     'environ',
                     'max_x',
                     'little_bit',
                     'sse',
                     'output_type',
                     'max_z',
                     'cni',
                     'save_tensor',
                     'max_y']
    
    ignore_exception = boolean_parameter('ignore_exception', False)
    terminal_output = Parameter('terminal_output', 'dropdown', str, 'stream',
            items = ['stream', 'allatonce', 'file', 'none'])
    args = text_parameter('args', '')
    base_name = text_parameter("base_name", "dtifit_")



##############################################################################
# Data Sink
##############################################################################

datasink_interface = nio.DataSink()

class DataSink5(Unit):
    interface = datasink_interface
    tag = "Sinks"
    instance_name_template = "datasink"
    redirected_ports_number = {'in': 2, 'out': 0}

    hidden_in_ports = ['ignore_exception',
                        'strip_dir',
                        'remove_dest_dir',
                        '_outputs',
                        'substitutions',
                        'parameterization',
                        'regexp_substitutions']

    ignore_exception = boolean_parameter('ignore_exception', False)
    parameterization = boolean_parameter('parameterization', True)
    remove_dest_dir = boolean_parameter('remove_dest_dir', False)
    base_directory = path_parameter('base_directory', 'test_data')



def get_unit_types():
    # list of all classes that are visible from the GUI
    return [PrimitiveSource,
            IterableSource,
            TypeConverter,
            DTIDataSource,
            ROIExtractor,
            BrainExtractor,
            DTIFitter,
            DataSink5]

