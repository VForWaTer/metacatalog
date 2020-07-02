"""
.. note::

    The extension management of metacatalog was completely rewritten in 
    version 0.1.12. A detailed description will follow once the changes
    get stable with version 0.2

Since version 0.2 metacatalog uses an extension system to 
load functionality, that is not part of the core metacatalog 
features. The package itself uses it to add data I/O operations
and export functionality.

Every extension needs at least an Interface class. After this class was 
imported, you can add it using the :func:`extension` function, or add 
it permanently using the :func:`activate_extension`

"""
from .base import MetacatalogExtensionInterface

EXTENSIONS = dict()


def extension(name: str, interface: MetacatalogExtensionInterface =None):
    """
    Register or Return Extension Interface for metacatalog.
    If :attr:`interface` is None, the extension interface 
    class of given name will be returned.
    If :attr:`interface` is not None, it will be registered 
    as given :attr:`name`. On register, an init_extension 
    classmethod will be executed, if given.

    Parameters
    ----------
    name : str
        Extension name. You can use any name you want and 
        the :func:`extension` function will return the 
        passed :attr:`interface` on next call. 
        If other extensions or custom code rely on the 
        extension, choose a common name.
    interface : MetacatalogExtensionInterface
        To be a valid interface, a class needs to inherit
        from MetacatalogExtensionInterface and as of now 
        implement the abstract method init_extension. 
        This will be run on register.
    
    """
    if interface is None:
        if name not in EXTENSIONS.keys():
            raise AttributeError("'%s' is not a known metacatalog extension." % name)
        return EXTENSIONS[name]
    else:
        interface.init_extension()
        EXTENSIONS[name] = interface


def __load_extensions():
    """
    It is possible to configure to load 
    """
    from metacatalog import CONFIGFILE
    import json
    import importlib

    with open(CONFIGFILE, 'r') as f:
        config = json.load(f)
    
    if not 'extensions' in config:
        config['extensions'] = dict()
    
    for name, ex in config['extensions'].items():
        try:
            mod = importlib.import_module(ex['module'])
            interfaceCls = getattr(mod, ex['interface'])
            extension(name, interfaceCls)
        except Exception as e:
            print("Error on loading Extension '%s'\n%s" % (name, str(e)))

def activate_extension(name: str, module_name: str, interface_class_name: str):
    """
    Permanently activate an extension in this metacatalog instance.
    It has to be accessible as a python module. give the full name as 
    :attr:`module_name`

    It will be registered as :attr:`name` and can therefore be loaded by 

    .. code-block:: python
        from metacatalog.ext import extension
        extension(name)
    

    """
    from metacatalog import CONFIGFILE
    import json

    with open(CONFIGFILE, 'r') as f:
        config = json.load(f)
    
    if not 'extensions' in config:
        config['extensions'] = dict()
    
    # set the new extension
    config['extensions'][name] = {'module': module_name, 'interface': interface_class_name}

    # save
    with open(CONFIGFILE, 'w') as f:
        json.dump(config, f, indent=4)
    
    # now load
    print('Extension is activated, you need to reload metacatalog be be effective.')
