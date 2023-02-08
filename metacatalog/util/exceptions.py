class MetadataMissingWarning(RuntimeWarning):
    pass


class MetadataMissingError(RuntimeError):
    pass


class IOOperationNotFoundError(RuntimeError, ValueError):
    pass


class StandardsExportExtentionNotFoundError(RuntimeError, ValueError):
    pass