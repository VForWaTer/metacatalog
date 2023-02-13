r"""
The basic mechanism to add new extensions to metacatalog is to 
create an Interface class and call the adding method of metacatalog.

A new interface class has to inherit from :class:`MetacatalogExtensionInterface`
and can be added like:

.. code-block:: python

    class DummyExtension(MetacatalogExtensionInterface):
        def init_extension(self):
            pass
    
    from metacatalog.ext import extension
    
    extension('dummy', DummyExtension)


You can do almost anything within the init function of the 
Extension. E.g. you can add a new method to the Entry model:

.. code-block:: python

    from metacatalog.models import Entry
    import json

    def save(self):
        with open('test.json', 'w') as js:
            json.dump(self.to_dict(), js)
        
    class SaveToFileExtension(MetacatalogExtensionInterface):
        @classmethod
        def init_extension(cls):
            setattr(Entry, 'saveToJson', save)


More complicated examples are also possible. The following example will 
re-implement the ``__init__`` function of :class:`Entry <metacatalog.models.Entry>` 
to add a print statement on each initialization. The difference here is, that 
the old ``__init__`` functions is copied and executed inside the new 
``__init__`` function.

.. code-block:: python
    
    from metacatalog.models import Entry
    from metacatalog import ext

    class PrintExtension(ext.MetacatalogExtensionInterface):
        @classmethod
        def init_extension(cls):
            init = Entry.__init__
            def new_init(self, *args, **kwargs):
                init(self, *args, **kwargs)
                print('New Entry build')
            Entry.__init__ = init
    
    ext.extension('print', PrintExtension)

"""
import abc
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from argparse import _SubParsersAction, ArgumentParser


class MetacatalogExtensionInterface(abc.ABC):
    """
    Abstract Base Class for Metacatalog extensions.

    To create a new Metacatalog extension, you need define a 
    new interface class. This class needs at least implement 
    the :func:`init_extension`, which will be executed 
    as soon as the extension is added to metacatalog. For 
    this, you can use the :func:`metacatalog.ext.extension`
    function. 

    """
    @abc.abstractclassmethod
    def init_extension(cls):
        pass

    @classmethod
    def init_cli(cls, subparsers: '_SubParsersAction[ArgumentParser]', defaults: 'ArgumentParser') -> None:
        """
        Add a new :class:`ArgumentParser <argparse.ArgumentParser>` to the metacatalog CLI.
        The main CLI argument parser will call the ``init_cli`` class method of all active
        extensions and pass the main subparser to the init function. The second argument
        is the :class:`ArgumentParser <argparse.ArgumentParser>`, which holds the default
        arguments, that should affect all CLI functions.

        Example
        -------

        .. code-block:: Python

            from metacatalog.ext import MetacatalogExtensionInterface

            class MyExt(MetacatalogExtensionInterface):
                @classmethod
                def init_extension(cls):
                    pass
                
                @classmethod
                def cli(cls, args):
                    if not args.quiet:
                        print(args.foo.upper())
                    
                @classmethod
                def init_cli(cls, subparsers, defaults):
                    myparser = subparsers.add_parser('foobar', parents=[defaults], help="Just a foobar parser")
                    myparser.add_argument('foo', type=str, help="A nonsense argument")
                    myparser.set_defaults(func=MyExt.cli)

        """
        return None