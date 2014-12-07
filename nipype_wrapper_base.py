import nipype.pipeline.engine as pe
from nipype.interfaces.utility import Function
import nipype.interfaces.base as nibase
import numpy as np

from earlpipeline.backends import base

import pickle
from abc import ABCMeta

# Some default class variables to be set upon the NipypeWrapperUnit class
# creation. These defaults are applied via the metaclass
base_defaults = {
        # whether to check all in ports if they are also valid interfaces inputs
        'check_in_ports': True,

        # redirection
        'redirected_ports_number': None,
        'redirect_in_ports': False,
        'redirect_out_ports': False
        }

# String conventions
redir_port_template = "slot_%s" # 0: port number
redir_parameter_template = "%s_%s" # 0: port name; 1: port type (in/out)

# Metaclass for additional operations
class NipypeWrapperUnitMeta(ABCMeta):
    """Metaclass for creating a wrapper class around a Nipype
    interface object. It is inherited from ABCMeta metaclass to still
    allow for abstract methods"""

    def __init__(cls, name, bases, dct):
        """Check the class attributes of the newly created Unit class (not
        instance) and do the necessary preparations"""

        # pass over to the parent metaclass
        super(NipypeWrapperUnitMeta, cls).__init__(name, bases, dct)

        # do additional stuff only if interface variable is specified
        if dct.has_key('interface'):
            # set the defaults
            for k, v in base_defaults.items():
                if not dct.has_key(k):
                    setattr(cls, k, v)

            # hide some ports by default
            cls.hidden_in_ports = ['_outputs']
            if dct.has_key('hidden_in_ports'):
                # add user supplied hidden ports, if given
                cls.hidden_in_ports += dct['hidden_in_ports']

            # check if the nipype interface is supplied, but not for the base class
            interface = dct['interface']
            #if not interface:
                #raise Exception("Please, specify nipype interface as a class attribute")
            if not isinstance(interface, nibase.Interface):
                raise Exception("Unknown interface type passed: %s, expected an instance of nipype.interfaces.base.Interface" % type(interface))

            # get a list of parameter objects
            parameter_names = []
            for attrname, attrvalue in dct.iteritems():
                if isinstance(attrvalue, base.Parameter):
                    parameter_names.append(attrname)

            
            # check if all parameters correspond to valid nipype inputs
            if cls.check_in_ports:
                input_traits = interface.inputs.trait_get()
                for p_name in parameter_names:
                    if not input_traits.has_key(p_name):
                        raise Exception("Passed parameter %s doesn't correspond to any input port of interface %s" % (p_name, repr(interface)))

            # Add parameter names to a list of excluded ports (they're not used
            # for dynamic data transfer)
            #cls.hidden_in_ports += parameter_names

            # if a special input attribute "logger_name" is present in the
            # interface and not a parameter, add it to the list of hidden ports
            logger_name = 'logger_name'
            if (interface.inputs.trait_get().has_key(logger_name)) \
                    and (not logger_name in parameter_names):
                cls.hidden_in_ports.append(logger_name)

            ####################
            # Port redirection #
            ####################
            # This feature is implemented for the cases where the name of the
            # earlPipeline port on the unit doesn't correspond to the name of
            # the respective nipype port. If the special variables are set
            # (i.e. redirected_ports_number), the connect and disconnect
            # methods of NipypeWrapperPipeline will detect the created flags
            # and transparently redirect the ports upon connection.
            # Furthermore, for every to-be-redirected port, a special parameter
            # is exposed in the GUI, a value of which specifies the name of the
            # nipype's in or out port, to which the corresponding earlPipeline
            # port needs to be redirected. 
            # 
            # The class with this metaclass should
            # have fields:
            #
            #   redirected_ports_number: a dict containing two integers for
            #      numbers of input and output ports respectively 
            if cls.redirected_ports_number:
                port_nums = cls.redirected_ports_number
                if port_nums['in']: cls.redirect_in_ports = True
                if port_nums['out']: cls.redirect_out_ports = True

                cls.redirected_out_ports = []
                cls.redirected_in_ports = []

                for port_type, port_num in port_nums.items():
                    for j in range(port_num):
                        # create a port name
                        port_name = redir_port_template%j
                        getattr(cls, 'redirected_'+port_type+'_ports').append(port_name)

                        # create a corresponding parameter
                        par_name = redir_parameter_template % (port_name, port_type)
                        setattr(cls, par_name, base.Parameter(par_name, 'text', str, port_name))



class NipypeWrapperUnit(base.GenericUnit):
    __metaclass__ = NipypeWrapperUnitMeta

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
        # descriptor (which is called inside by the getter method of the
        # parameters_info property)
        self.parameters_info

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
        #if isinstance(cls.interface, Function):
            #input_names = cls.interface._input_names
        if isinstance(cls.interface, nibase.Interface):
            input_names = cls.interface.inputs.trait_get().keys()
        else:
            raise Exception("Not implemented for interfaces of type %s" % type(cls.interface))

        if hasattr(cls, 'redirected_in_ports'):
            input_names += cls.redirected_in_ports

        # don't show hidden ports
        return [name for name in input_names if not name in cls.hidden_in_ports]

    @classmethod
    def get_out_ports(cls):
        if isinstance(cls.interface, nibase.Interface):
            # Try to obtain output ports from the secret `_outputs` method
            try:
                output_names = cls.interface._outputs().trait_get().keys()
                if hasattr(cls, 'redirected_out_ports'):
                    output_names += cls.redirected_out_ports
                return output_names
            except:
                raise Exception("Couldn't obtain output ports from %s"%cls.interface.output_spec.__name__)
        else:
            raise Exception("Not implemented for interfaces of type %s" % type(cls.interface))

    def get_parameter(self, name):
        # if it a static predefined nipype port, get it the normal way
        if hasattr(self._node.inputs, name):
            return getattr(self._node.inputs, name)
        else:
            # see if it is a dynamic port
            try:
                return self._node.inputs._outputs[name]
            except:
                raise AttributeError("Nipype node %s doesn't have parameter named %s" % (self._node.name, name))


            


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
        dest = self._units[dest_name]._node

        wf_src_port, wf_dest_port = self.handle_redirection(src_name, src_port, dest_name, dest_port)
        self._workflow.connect(src, str(wf_src_port), dest, str(wf_dest_port))

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

        wf_src_port, wf_dest_port = self.handle_redirection(src_name, src_port, dest_name, dest_port)
        self._workflow.disconnect(src, str(wf_src_port), dst, str(wf_dest_port))
        del self._edges[edge.id]

    def handle_redirection(self, src_name, src_port, dest_name, dest_port):
        """Due to dynamical nature of nipype's connectivity, we allow for
        dynamic redirections. This transparently redirects the earlPipeline static
        in/out port (e.g. "slot_1") to a dynamical nipype port (e.g.
        'dwi_files'), whose name is specified in
        src.inputs._output['slot_1_in/out']. The reason the strange
        _output variable is used is because this is how nipype interface's
        add_trait method works"""
        #import ipdb; ipdb.set_trace()
        wf_src_port, wf_dest_port = src_port, dest_port
        src_unit = self._units[src_name]
        dest_unit = self._units[dest_name]

        if src_unit.redirect_out_ports:
            wf_src_port = src_unit.get_parameter(redir_parameter_template % (src_port, 'out'))
        if dest_unit.redirect_in_ports:
            # experimental: add it to hidden ports
            wf_dest_port = dest_unit.get_parameter(redir_parameter_template % (dest_port, 'in'))

        return wf_src_port, wf_dest_port

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
