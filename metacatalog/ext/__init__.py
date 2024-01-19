"""
Since version 0.2 metacatalog uses an extension system to 
load functionality, that is not part of the core metacatalog 
features. The package itself uses it to add data I/O operations
and export functionality.

"""
from .base import MetacatalogExtensionInterface