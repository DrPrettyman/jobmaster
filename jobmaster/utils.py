import datetime
import inspect
import json
import re
import types


def wrap_string(_value):
    if isinstance(_value, str):
        return f'"{_value}"'
    return _value


def parse_params_from_doc(doc_string):
    if doc_string is None:
        return dict()
    _doc_lines = [_.strip() for _ in doc_string.split("\n")]
    params = dict()
    for _line in _doc_lines:
        if m := re.match(r":param (\w+): (.*)", _line):
            params[m.group(1)] = dict(help_str=m.group(2))
    return params


def parse_params_from_signature(signature: inspect.Signature):
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


def parse_argument_type(arg_str: str) -> dict:
    try:
        args = json.loads(arg_str)
    except json.JSONDecodeError:
        args = arg_str

    if isinstance(args, dict):
        return args

    _out = dict()
    for _pair in args.strip('{}').strip('"')[1:-1].split(')","('):
        first_comma = _pair.index(',')
        _key, _json_value = _pair[:first_comma], _pair[first_comma + 1:]

        try:
            _v = json.loads(_json_value)
        except json.JSONDecodeError:
            _v = _json_value.strip('\\"')

        _out[_key] = _v

    return _out


class StdLogger:
    def __init__(self, name: str = "JobMaster"):
        self.name = name

    def debug(self, msg):
        print(f"{datetime.datetime.now()} | DEBUG {self.name}: {msg}")

    def info(self, msg):
        print(f"{datetime.datetime.now()} | INFO {self.name}: {msg}")

    def warning(self, msg):
        print(f"{datetime.datetime.now()} | WARNING {self.name}: {msg}")

    def error(self, msg):
        print(f"{datetime.datetime.now()} | ERROR {self.name}: {msg}")

    def critical(self, msg):
        print(f"{datetime.datetime.now()} | CRITICAL {self.name}: {msg}")


class NothingLogger:
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass

    def critical(self, msg):
        pass
