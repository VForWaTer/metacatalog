class MetadataMissingWarning(RuntimeWarning):
    pass


class MetadataMissingError(RuntimeError):
    pass


class IOOperationNotFoundError(RuntimeError, ValueError):
    pass


class StandardExportExtentionNotFoundError(RuntimeError, ValueError):
    pass