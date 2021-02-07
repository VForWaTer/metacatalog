from datetime import datetime


def flatten(d: dict, sep='.') -> dict:
    """
    Flatten a dictionary by concatenating 
    parent and child keys by :attr:`sep` for 
    nested dictionaries.
    """
    out = dict()
    for k,v in d.items():
        if isinstance(v, dict):
            out.update(__flatten(k, v, sep=sep))
        elif isinstance(v, list):
            out.update(__flat_list(k, v, sep=sep))
        else:
            out[k] = v
    return out

def __flatten(key, d: dict, sep='.'):
    out = dict()
    for k,v in d.items():
        if isinstance(v, dict):
            out.update(__flatten(k, v, sep=sep))
        else:
            out['{parent}{sep}{child}'.format(parent=key, sep=sep, child=k)] = v
    return out


def __flat_list(key, l: list, sep='.') -> dict:
    out = dict()
    for num, value in enumerate(l):
        k = '{key}{sep}{num}'.format(key=key, sep=sep, num=num)
        if isinstance(value, dict):
            out.update(__flatten(k, value, sep=sep))
        elif isinstance(value, list):
            out.update(__flat_list(k, value, sep=sep))
        else:
            out[k] = value
    return out


def serialize(d, stringify=False) -> dict:
    """
    Serializes an object to json. Currently it can 
    convert datetime to isoformat and any of the 
    metacatalog.models, which holds an :attr:`to_dict` 
    function.
    """
    if isinstance(d, dict):
        return {k: __convert(v, stringify=stringify) for k,v in d.items()}
    elif hasattr(d, 'to_dict'):
        return serialize(d.to_dict(), stringify=stringify)
    else:
        return d


def __convert(value, stringify=False): 
    if isinstance(value, dict):
        return serialize(value, stringify=stringify)
    elif hasattr(value, 'to_dict'):
        return __convert(value.to_dict(), stringify=stringify)
    elif isinstance(value, datetime):
        return value.isoformat()
    else:
        if stringify:
            return str(value)
        else:
            return value
