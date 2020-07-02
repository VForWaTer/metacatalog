from copy import deepcopy

from sqlalchemy.orm import reconstructor
from sqlalchemy import event

from .interface import IOExtensionInterface
from metacatalog.models import Entry


class IOExtension(IOExtensionInterface):
    """
    Input / Output extension.
    This is the default extension for all kind of CRUD operations on the 
    actual data described by metacatalog. It can be used on two different 
    levels. As a class, it offers classmethods to add and return new 
    functions for performing the actual
    """
    @classmethod
    def init_extension(cls):
        """
        Add the IOExtension as an attribute to 
        the Entry model
        """
        # set the Extension interface
        Entry.io_interface = IOExtension
        
        # define an event to listen for Entry load events
        @event.listens_for(Entry, 'load')
        def init_io_ext(instance, context):
            instance.foo = 'bar'
            instance.io_extension = instance.io_interface(instance)

    def read(self, **kwargs):
        return super(IOExtension, self).read(**kwargs)
    
    def import_(self, data, **kwargs):
        return super(IOExtension, self).import_(data, **kwargs)

    def append(self, data, **kwargs):
        return super(IOExtension, self).append(data, **kwargs)
    
    def delete(self, **kwargs):
        return super(IOExtension, self).delete(**kwargs)