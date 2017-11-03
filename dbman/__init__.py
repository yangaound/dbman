# -*- coding: utf-8 -*-
'''
Source:
    https://github.com/yangaound/xmlutil

Created on 2016年12月24日

@author: albin
'''

import re
import abc
from collections import defaultdict
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

import petl
try:
    from lxml import etree
except ImportError:
    from xml.etree import cElementTree as etree
except ImportError:
    from xml.etree import ElementTree as etree

__version__ = '1.0.0'

namespace_pattern = re.compile(r"{.+}")


def parse(filename, *args, **kwargs):
    element = etree.parse(filename, *args, **kwargs).getroot()
    return XMLNode(element)


def get_tag(element):
    """return the element's local name"""
    return re.sub(namespace_pattern, '', element.tag)


def get_namespace(element):
    """return the element's namespace"""
    return re.sub(get_tag(element), '', element.tag)


class BridgeNode(object):
    """Abstract class, it wraps an instance of ``lxml.etree.Element`` or ``xml.etree.ElementTree.Element`` as a implementor"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, element):
        if element is None:
            raise TypeError("Argument 'element' should be an instance of lxml.etree.Element or xml.etree.ElementTree.Element")
        self.element = element

    @abc.abstractmethod
    def to_dicts(self, **kwargs):
        """expands the wrapped element tree into a ``sequence``.
        :return: ``list<dict>``"""

    def to_table(self, inclusive_tags=(), exclusive_tags=(), duplicate_tags=(), with_element=False, with_attrib=False, ):
        dicts = self.to_dicts(duplicate_tags=duplicate_tags, with_element=with_element, with_attrib=with_attrib, )
        table = dicts2table(dicts)  
        if inclusive_tags:
            header = [field for tag in inclusive_tags for field in table.header() if (tag == field) or field.startswith(tag + '_')]
            table = table.cut(*header)
        elif exclusive_tags:
            header = [field for tag in exclusive_tags for field in table.header() if (tag == field) or field.startswith(tag + '_')]
            table = table.cutout(*header)    
        return table
            
    def findall(self, expression, **kwargs):
        """Wraps the result of executing expression into a ``GroupNode`` and return it"""
        return self._execute_expression(self.element, 'findall', expression, **kwargs)

    def xpath(self, expression, **kwargs):
        """Wraps the result of executing expression into a ``GroupNode`` and return it"""
        return self._execute_expression(self.element, 'xpath', expression, **kwargs)

    def _execute_expression(self, target_node, func_name, expression, **kwargs):
        """executes expression over target_node methods named func_name"""
        func = getattr(target_node, func_name)
        elements = func(expression, **kwargs)
        return GroupNode((XMLNode(e) for e in elements))

    def join(self, other, key=None, **petl_kwargs):
        """join this node and other node as a ``RelatedNode`` """
        return self.relate(other, 'join', key=key, **petl_kwargs)

    def crossjoin(self, other, **petl_kwargs):
        """crossjoin this node and other node as a ``RelatedNode`` """
        return self.relate(other, 'crossjoin', **petl_kwargs)

    def relate(self, other, relation, **petl_kwargs):
        """relate this node and other node as a ``RelatedNode`` over relation. relation is a function name of petl package"""
        return RelatedNode(self, other, relation, **petl_kwargs)

    def tag(self):
        return get_tag(self.element)

    def namespace(self):
        return get_namespace(self.element)
        
    def nsmap(self, sub=None):
        nsmap = self.element.nsmap
        return dict((k if k else sub, v) for k, v in nsmap.items())

    def __repr__(self):
        return "<%s %s at 0x%x>" % (self.__class__.__name__, self.tag(), id(self))


class EmptyNode(BridgeNode):
    """This class just use to relates another nodes"""

    def __init__(self): pass

    def to_dicts(self, **kwargs):
        """implement"""
        return []

    def __repr__(self):
        return "<%s %s at 0x%x>" % (self.__class__.__name__, 'None', id(self))


class XMLNode(BridgeNode):
    """This class wraps an instance of ``lxml.etree.Element`` or ``xml.etree.ElementTree.Element`` as a implementor"""

    def to_dicts(self, **kwargs):
        """implement"""
        dicts = DFSExpansion(self.element, **kwargs).expand()
        return dicts

    def remove(self):
        parent = self.element.getparent()
        parent.remove(self.element)

    def find(self, expression, **kwargs):
        element = self.element.find(expression, **kwargs)
        return XMLNode(element)


class GroupNode(BridgeNode, list):
    """This class wraps a not empty collection which type must be ``Iteration<? extends xmlutil.BridgeNode>``"""

    def __init__(self, nodes):
        self.extend(nodes)
        BridgeNode.__init__(self, self[0].element)

    def to_dicts(self, **kwargs):
        """implement"""
        dicts = []
        for node in self:
            dicts.extend(node.to_dicts(**kwargs))
        return dicts

    def _execute_expression(self, _, func_name, expression, **kwargs):
        """overwrite"""
        nodes = []
        for node in self:
            nodes.extend(BridgeNode._execute_expression(self, node, func_name, expression, **kwargs))
        return GroupNode(nodes)

    def remove(self):
        for node in self:
            node.remove()


class RelatedNode(BridgeNode):
    """This class wraps 2 node over their relation, which type must be ``extends xmlutil.BridgeNode``"""

    def __init__(self, this, other, relation, **kwargs):
        super(RelatedNode, self).__init__(other.element)
        self.this = this
        self.other = other
        self.relation = relation
        self.kwargs = kwargs

    def to_dicts(self, **kwargs):
        """implement"""
        return self.to_table(**kwargs).dicts()

    def to_table(self, **kwargs):
        """overwrite"""            
        this_table = dicts2table(self.this.to_dicts(**kwargs))
        other_table = dicts2table(self.other.to_dicts(**kwargs))
        if this_table.nrows() == 0:       # self.this is a EmptyNode
            table = other_table
        else:
            table = getattr(this_table, self.relation)(other_table, **self.kwargs)
        return table

    def _execute_expression(self, _, func_name, expression, **kwargs):
        """overwrite"""
        nodes1 = BridgeNode._execute_expression(self, self.this, func_name, expression, **kwargs)
        nodes2 = BridgeNode._execute_expression(self, self.other, func_name, expression, **kwargs)
        return GroupNode(nodes1 + nodes2)


def dicts2table(dicts):
    """transform dicts into a ``petl.util.base.Table``"""
    return petl.wrap(petl.fromdicts(dicts)) if dicts else petl.empty()


class DFSExpansion(object):
    """depth first search element tree and expands it into a ``sequence`` of ``dict``
    :type element: ``lxml.etree.Element`` or ``xml.etree.cElementTree.Element``
    :param duplicate_tags: elements with these tag will be renamed and added to a dictionary
    :param with_element: the values of dictionaries contains of element if True otherwise contains of element's text.
    :param with_attrib: element that's attribute is not empty will be added to dictionaries if with_attrib is True

    E.g.,
    >>> expansion = DFSExpansion(element)
    >>> dicts = expansion.expand()
    >>> for dic in dicts:
    >>>     print dic
    """

    def __init__(self, element, duplicate_tags=(), with_element=False, with_attrib=False):
        self.element = element
        self.duplicate_tags = duplicate_tags
        self.duplicate_tags_counter = None
        self.with_element = with_element
        self.with_attrib = with_attrib
        self.dicts = list()
        self.buffer_tags = list()
        self.buffer_dict = OrderedDict()

    def expand(self):
        self._reset_duplicate_tags_counter()
        self._expand(self.element)
        return self.dicts

    def _expand(self, element, level=0):
        tag, text = get_tag(element), element.text.strip() if element.text else None
        if text:                                     # element.text is not empty:
            self._buffer(tag, text, element)
        elif element.attrib and self.with_attrib:    # element.attribute is not empty and need to fetch it
            self._buffer(tag, text, element, with_attrib=True)
        for e in element:
            self._expand(e, level + 1)
        if level == 0:
            self._insert(tag)

    def _buffer(self, tag, text, element, with_attrib=False):
        if self.buffer_dict.get(tag) is not None:
            if self.duplicate_tags and tag in self.duplicate_tags:
                self.duplicate_tags_counter[tag] += 1
                tag = tag + '_' + str(self.duplicate_tags_counter[tag])
            else:
                self._insert(tag)
        self.buffer_tags.append(tag)
        buffering = element if self.with_element else (element.attrib if with_attrib else text)
        self.buffer_dict.update({tag: buffering})

    def _reset_duplicate_tags_counter(self):
        self.duplicate_tags_counter = defaultdict(int)

    def _insert(self, duplicate_tag):
        self._reset_duplicate_tags_counter()
        self.dicts.append(self.buffer_dict.copy())
        try:
            idx = self.buffer_tags.index(duplicate_tag)
        except ValueError:
            idx = len(self.buffer_tags)
        for tag in self.buffer_tags[idx:]:
            self.buffer_dict.has_key(tag) and self.buffer_dict.update({tag: None})
        self.buffer_tags = self.buffer_tags[:idx]

