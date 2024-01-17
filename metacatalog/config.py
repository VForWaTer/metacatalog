from typing import Any, Dict, Union
from pydantic import BaseModel, PostgresDsn, Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import importlib
import warnings

from metacatalog.ext.base import MetacatalogExtensionInterface

# always load a .env file if there is one
# The config object will then be instantiated afterwards
load_dotenv()

DEFAULT_EXTENSIONS = ','.join([
    'io:metacatalog.ext.io.extension.IOExtension',
])


class ExtensionLoadError(RuntimeError):
    pass

class Extension(BaseModel):
    name: str
    interface = Field(MetacatalogExtensionInterface, repr=False)

    @classmethod
    def from_interface_path(cls, name: str, path: str) -> 'Extension':
        try:
            [*import_path, interface_name] = path.split('.')
            mod = importlib.import_module('.'.join(import_path))
            interface = getattr(mod, interface_name)
        except ImportError as e:
            raise ExtensionLoadError(f"Could not load Extension {name}. Please install: {str(e)}")
        except AttributeError as e:
            raise ExtensionLoadError(f"Could not find interface {interface_name} in {import_path}. Message: {str(e)}")
        except Exception as e:
            raise ExtensionLoadError(f"Error on loading Extension: {str(e)}.")
        
        # return the instance
        return Extension(name=name, interface=interface)

    def init(self):
        self.interface.init_extension()
    
    def init_extension(self):
        self.init()


class  Config(BaseSettings):
    connection: PostgresDsn = Field(default='postgresql://postgres:postgres@localhost:5432/metacatalog', alias='METACATALOG_URI')
    extensions: Field(default='', alias='METACATALOG_EXTENSIONS')
    active_extensions: Dict[str, Extension] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        # read in all extensions requested by the user in addition 
        # to the default extensions. Read the default first, so that
        # the user can overwrite them.
        req_extensions = f"{DEFAULT_EXTENSIONS},{self.extensions}".split(',')

        # load all extensions
        for req in req_extensions:
            if req == '':
                continue
            name, interface = req.split(':')
            self.load_extension(name, interface)


        return super().model_post_init(__context)

    def load_extension(self, name: str, interface_path: Union[str, MetacatalogExtensionInterface]):
        """
        Load an extension by name and interface. 
        The interface is a class that inherits from 
        :class:`metacatalog.ext.base.MetacatalogExtensionInterface`.
        """
        # if the interface path is a string, import it
        if isinstance(interface_path, str):
            try:
                extension = Extension.from_interface_path(name, interface_path)
            except ExtensionLoadError as e:
                warnings.warn(str(e))
        else:
            extension = Extension(name=name, interface=interface_path)
        
        # init the interface
        extension = extension.init()

        # add to the extensions
        self.active_extensions[name] = extension

    def extension(self, name: str) -> Extension:
        if name not in self.extensions:
            raise AttributeError(f"'{name}' is not a known metacatalog extension.")
        return self.extensions[name]


# instantiate a default config object
config = Config()
