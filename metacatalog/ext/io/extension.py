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

        # overwrite the Entry.__init__
        entry_init = Entry.__init__

        # define the new init method
        def init(self, *args, **kwargs):
            entry_init(self, *args, **kwargs)
            # init an Instance of IOExtension bound to Entry
            self.io_extension = IOExtension(self)
        Entry.__init__ = init

    def read(self, **kwargs):
        super(IOExtension, self).read(**kwargs)
    
    def import_(self, data, **kwargs):
        super(IOExtension, self).import_(data, **kwargs)

    def append(self, data, **kwargs):
        super(IOExtension, self).append(data, **kwargs)
    
    def delete(self, **kwargs):
        super(IOExtension, self).delete(**kwargs)