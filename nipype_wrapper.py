import nipype.pipeline.engine as pe
from nipype.interfaces.utility import Function

from earlpipeline.backends import base

import pickle

def Hello():
    import os
    from nipype import logging
    import time

    iflogger = logging.getLogger('interface')
    message = "Hello "
    file_name =  'hello.txt'
    iflogger.info(message)
    iflogger.info(os.path.abspath(file_name))
    with open(file_name, 'w') as fp:
        fp.write(message)

    time.sleep(3)

    return os.path.abspath(file_name)

def World(in_file, wrepeat):
    from nipype import logging
    import time
    iflogger = logging.getLogger('interface')
    message = str(["World!" for i in range(wrepeat)])
    iflogger.info(message)
    with open(in_file, 'a') as fp:
        fp.write(message)

    time.sleep(3)

def Collect(if1, if2, if3, if4):
    print if1+if2+if3+if4

hello_iface=Function(input_names=[],
                  output_names=['out_file'],
                  function=Hello)

world_iface=Function(input_names=['in_file', 'wrepeat'],
                  output_names=[],
                  function=World)

collect_iface=Function(input_names=['if1', 'if2', 'if3', 'if4'],
                  output_names=[],
                  function=Collect)


class Unit(base.GenericUnit):
    # overload this please!
    interface = None
    def __init__(self):
        super(Unit, self).__init__()
        self._pipeline = None

    def initialize(self, name, *args, **kwargs):
        """Initializes underlying nipype.Node instance"""
        self._node = pe.Node(self.interface, name, *args, **kwargs)

    @property
    def name(self):
        return self._node.name

    @property
    def pipeline(self):
        if self._pipeline:
            return self._pipeline
        else:
            raise Exception("%s doesn't belong to any pipeline pipeline" % self.name)

    @classmethod
    def get_in_ports(cls):
        if isinstance(cls.interface, Function):
            # don't show ports which are parameters
            return [p for p in cls.interface._input_names if not p in cls.parameter_names]
        else:
            raise Exception("Not implemented for other classes")

    @classmethod
    def get_out_ports(cls):
        if isinstance(cls.interface, Function):
            return cls.interface._output_names
        else:
            raise Exception("Not implemented for other classes")

    def get_parameter(self, name):
        return getattr(self._node.inputs, name)

    def set_parameter(self, name, value):
        self._node.inputs.set(**{name: value})

class Pipeline(base.GenericPipeline):
    def __init__(self, name, *args, **kwargs):
        super(Pipeline, self).__init__()
        self._workflow = pe.Workflow(name, *args, **kwargs)
        self._units = {} # name:Unit()
        self._edges = {} # id:Edge()

    @property
    def name(self):
        return self._workflow.name

    @property
    def units(self):
        return self._units.values()

    @property
    def edges(self):
        return self._edges.values()

    def get_unit(self, unit_name):
        return self._units[unit_name]

    def add_unit(self, unit, unit_name, *args, **kwargs):
        self._units[unit_name] = unit

        if unit._pipeline:
            raise Exception("Unit '%s' already belongs to pipeline '%s'" % (unit.name, unit._pipeline))
        else:
            unit._pipeline = self

        unit.initialize(unit_name, *args, **kwargs)
        self._workflow.add_nodes([unit._node])

    def remove_unit(self, unit_name):
        unit = self._units[unit_name]
        self._workflow.remove_nodes([unit._node])
        del self._units[unit_name]

    def connect(self, src_name, src_port, dest_name, dest_port):
        src = self._units[src_name]._node
        dst = self._units[dest_name]._node

        self._workflow.connect(src, str(src_port), dst, str(dest_port))

        edge = base.Edge(src_name, src_port, dest_name, dest_port)
        self._edges[edge.id] = edge

        return edge

    def find_edge(self, src_name, src_port, dest_name, dest_port):
        # create dummy edge
        edge = base.Edge(src_name, src_port, dest_name, dest_port)
        if self._edges.has_key(edge.id):
            return self._edges[edge.id]
        else:
            raise Exception("Edge '%s' not found!" % edge.id)

    def disconnect(self, src_name, src_port, dest_name, dest_port):     
        edge = self.find_edge(src_name, src_port, dest_name, dest_port)

        src = self._units[src_name]._node
        dst = self._units[dest_name]._node

        self._workflow.disconnect(src, str(src_port), dst, str(dest_port))
        del self._edges[edge.id]

    def run(self):
        def status_callback(node, nip_status):
            unit = self.get_unit(node.name)
            if nip_status == 'start':
                status = base.tools.Status.RUNNING
            elif nip_status == 'end':
                status = base.tools.Status.FINISHED
            else:
                status = base.tools.Status.FAILED

            unit.status = status
            
        self._workflow.run(plugin='MultiProc', plugin_args={'status_callback': status_callback, 'n_procs':4})


    # TODO: Think about it a lot, and then remove it
    @classmethod
    def save(cls, ppl, fname):
        """get the state representation of the current pipeline, and write it
        to a file. It is a hacky parody on a pickle protocol, should be
        eliminated with fire ASAP"""

        if not isinstance(ppl, cls):
            raise Exception("Cannot save pipeline: the passed pipeline instance has wrong type")

        # nodes list in the form [(name, class_creation_func, class_creation_func_args, parameters)]
        units = []
        for uname, unit in ppl._units.items():
            parameters = {}

            # unit parameters (functional parameters)
            for pname, p in unit.parameters_info.items():
                parameters[pname] = p['value']

            # add other miscellaneous attributes
            parameters.update({'top': unit.to_dict()['top'],
                'left': unit.to_dict()['left']})

            #units.append((uname, unit.__class__.__name__, parameters))
            unit_state = {'name':uname,
                    'class_creation_func_args': unit.__class__.class_creation_func_args,
                    'parameters': parameters}

            units.append(unit_state)
        
        # edge list
        edges = ppl._edges.values()

        # the resulting state object
        state = {'name': ppl.name,
                'units': units,
                'edges': edges}

        with open(fname, 'w') as f:
            pickle.dump(state, f)

    @classmethod 
    def load(cls, fname):
        """read the state representation from file and build a pipeline based
        on that information"""

        with open(fname) as f:
            state = pickle.load(f)

        ppl = Pipeline(state['name'])

        for unit_state in state['units']:
            # create unit class
            ucls = create_nipype_unit_class(*unit_state['class_creation_func_args'])

            # create empty instance
            unit = ucls()

            # add to pipeline
            ppl.add_unit(unit, unit_state['name'])

            # load state (i.e. parameter values and other attributes)
            #import ipdb; ipdb.set_trace()
            for pname, pvalue in unit_state['parameters'].items():
                setattr(unit, pname, pvalue)

        # connect the units
        for edge in state['edges']:
            ppl.connect(edge.src, edge.srcPort, edge.dst, edge.dstPort)

        return ppl


        

# class factory

def create_nipype_unit_class(interface_name, interface, parameters=[], **class_attrs):
    input_traits = interface.inputs.traits()
    parameter_dict = {}

    # check if all properties correspond to valid nipype inputs
    for p in parameters:
        if isinstance (p, basestring):
            p_name = p
            p_args = {}
        elif isinstance(p, tuple):
            p_name, p_args = p
        else:
            raise Exception("Wrong parameter type passed. Expected tuple or string, got %s" % type(p))
        if not input_traits.has_key(p_name):
            raise Exception("Passed parameter %s doesn't correspond to any input port of interface %s" % (p_name, interface_name))

        # if arguments to the Parameter descriptor are not specified, try to
        # guess type and default value from the corresponding trait
        # specification
        if not p_args:
            p_type = type(getattr(interface.inputs, p_name))
            if not p_type in [basestring, int, float, bool]:
                raise Exception("Parameter %s has no default value or unknown datatype. Pass additional arguments if you want to specify them manually" % p_name)

            p_args = {'name': p_name,
                      'parameter_type': 'text',
                      'value_type': p_type,
                      'default_value': getattr(interface.inputs, p_name)}

        parameter_dict[p_name] = base.Parameter(**p_args)

    attributes = {'interface': interface,
                  'parameter_names': parameter_dict.keys(),
                  'class_creation_func_args': (interface_name, interface, parameters)}
    attributes.update(parameter_dict)
    attributes.update(class_attrs)

    unit_class = type(interface_name, (Unit,), attributes)

    return unit_class


HelloUnit = create_nipype_unit_class('HelloUnit', hello_iface, tag="Sources")
WorldUnit = create_nipype_unit_class('WorldUnit', world_iface, tag="Sinks",
        parameters=[('wrepeat', {'name':'wrepeat',
            'parameter_type':'input',
            'value_type':int,
            'default_value':4,
            'datatype':'number'})])
CollectUnit = create_nipype_unit_class('CollectUnit', collect_iface, tag="Sinks",
        parameters=[('if4', {
            'name': 'if4',
            'parameter_type':'boolean',
            'value_type':bool,
            'default_value':False,
            }),
            
            ('if3', {
                'name': 'if3',
                'parameter_type':'text',
                'value_type':str,
                'default_value':'awesome',
                })])


# method, returning types
def get_unit_types():
    return [HelloUnit, WorldUnit, CollectUnit]





