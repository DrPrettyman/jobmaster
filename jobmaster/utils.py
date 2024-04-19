import datetime
import inspect
import json
import re
import types


def wrap_string(_value):
    if isinstance(_value, str):
        return f"'{_value}'"
    return _value


def _parse_params_from_doc(doc_string):
    _doc_lines = [_.strip() for _ in doc_string.split("\n")]
    params = dict()
    for _line in _doc_lines:
        if m := re.match(r":param (\w+): (.*)", _line):
            params[m.group(1)] = dict(help=m.group(2))
    return params


def _parse_params_from_signature(signature: inspect.Signature):
    params = dict()
    for _name, _param in signature.parameters.items():

        _default = _param.default
        if _default == inspect._empty:
            _default = None
            _required = True
        else:
            _required = False

        _select_from = None
        _type = None
        if _param.annotation is None:
            _type = type(None).__name__
        elif isinstance(_param.annotation, types.UnionType | types.GenericAlias):
            _type = str(_param.annotation)
        elif isinstance(_param.annotation, type):
            _type = _param.annotation.__name__
        elif isinstance(_param.annotation, list | tuple):
            _select_from = list(_param.annotation)
            _types = set(type(x) for x in _param.annotation)
            if len(_types) == 1:
                _type = _types.pop().__name__

        params[_name] = dict(
            name=_name,
            value_type=_type,
            select_from=_select_from,
            default_value=_default,
            required=_required
        )
    return params


def parse_argument_type(arg):
    _out = []
    for _pair in arg.strip('{}').strip('"')[1:-1].split(')","('):
        first_comma = _pair.index(',')
        _key, _json_value = _pair[:first_comma], _pair[first_comma + 1:]

        try:
            _v = json.loads(_json_value)
        except json.JSONDecodeError:
            _v = _json_value.strip('\\"')

        _out.append((_key, _v))
    return _out
