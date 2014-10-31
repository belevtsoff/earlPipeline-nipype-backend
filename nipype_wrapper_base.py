import nipype.pipeline.engine as pe
from nipype.interfaces.utility import Function
from nipype.interfaces.base import Interface

from earlpipeline.backends import base

import pickle
from abc import ABCMeta

# Metaclass for additional operations
class NipypeNodeWrapperType(ABCMeta):
    """Metaclass for creating a wrapper class around a Nipype
    interface object. It is inherited from ABCMeta metaclass to still
    allow for abstract methods"""

    def __init__(cls, name, bases, dct):
        """Check the class attributes of the newly created Unit class (not
        instance) and do the necessary preparations"""

        # pass over to the parent metaclass
        super(NipypeNodeWrapperType, cls).__init__(name, bases, dct)

        # do additional stuff only for inherited classes
        if name != 'NipypeWrapperUnit':
            # check if the nipype interface is supplied, but not for the base class
            interface = dct['interface']
            if not interface:
                raise Exception("Please, specify nipype interface as a class attribute")
            elif not isinstance(interface, Interface):
                raise Exception("Unknown interface type passed: %s, expected an instance of nipype.interfaces.base.Interface" % type(interface))

            # get a list of parameter objects
            parameter_names = []
            for attrname, attrvalue in dct.iteritems():
                if isinstance(attrvalue, base.Parameter):
                    parameter_names.append(attrname)

            
            # check if all parameters correspond to valid nipype inputs
            input_traits = interface.inputs.traits()
            for p_name in parameter_names:
                if not input_traits.has_key(p_name):
                    raise Exception("Passed parameter %s doesn't correspond to any input port of interface %s" % (p_name, repr(interface)))

            # store parameter names in a list to exclude them from the list of
            # input ports
            cls.hidden_in_ports = parameter_names

            # if a special input attribute "logger_name" is present in the
            # interface and not a parameter, add it to the list of hidden ports
            logger_name = 'logger_name'
            if (interface.inputs.traits().has_key(logger_name)) \
                    and (not logger_name in parameter_names):
                cls.hidden_in_ports.append(logger_name)


class NipypeWrapperUnit(base.GenericUnit):
    __metaclass__ = NipypeNodeWrapperType

    # overload this please!
    interface = None

    # names of the attributes which will not be shown as ports
    hidden_in_ports = []

    def __init__(self):
        super(NipypeWrapperUnit, self).__init__()
        self._pipeline = None
        if not hasattr(self, 'node_attrs'):
            self.node_attrs = {}

    def initialize(self, name):
        """Initializes underlying nipype.Node instance"""
        if not hasattr(self, 'node_type'):
            self._node = pe.Node(self.interface, name, **self.node_attrs)
        else:
            self._node = self.node_type(interface=self.interface, name=name, **self.node_attrs)

        # explicitly initialize parameters by invoking __get__ of the Parameter
        # descriptor
        for pname in self.parameters_info.keys():
            _ = getattr(self, pname)

        # if the interface has a "logger_name" attribute, set it to
        # the value of self.logger.name
        if hasattr(self.interface.inputs, 'logger_name'):
            self._node.inputs.logger_name = self.logger.name


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
            return [p for p in cls.interface._input_names if not p in cls.hidden_in_ports]
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

class NipypeWrapperPipeline(base.GenericPipeline):
    def __init__(self, name, *args, **kwargs):
        super(NipypeWrapperPipeline, self).__init__()
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
            
        #self._workflow.run()
        self._workflow.run(plugin_args={'status_callback': status_callback})
        #self._workflow.run(plugin='MultiProc', plugin_args={'status_callback': status_callback, 'n_procs':4})
        #self._workflow.run(plugin='IPython', plugin_args={'status_callback': status_callback})


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
                        'class': unit.__class__,
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

        ppl = NipypeWrapperPipeline(state['name'])

        for unit_state in state['units']:
            # create empty instance
            unit = unit_state['class']()

            # add to pipeline
            ppl.add_unit(unit, unit_state['name'])

            # load state (i.e. parameter values and other attributes)
            for pname, pvalue in unit_state['parameters'].items():
                setattr(unit, pname, pvalue)

        # connect the units
        for edge in state['edges']:
            ppl.connect(edge.src, edge.srcPort, edge.dst, edge.dstPort)

        return ppl