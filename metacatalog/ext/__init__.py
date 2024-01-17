"""
Since version 0.2 metacatalog uses an extension system to 
load functionality, that is not part of the core metacatalog 
features. The package itself uses it to add data I/O operations
and export functionality.

Every extension needs at least an Interface class. After this class was 
imported, you can add it using the :func:`extension` function, or add 
it permanently using the :func:`activate_extension`

"""