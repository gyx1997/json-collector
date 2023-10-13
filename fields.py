from typing import Callable, Any


class Field(object):
    """
    Represents a field of input data.
    """

    def __init__(self,
                 name: str,
                 raw_name: str = None,
                 default: Any = None,
                 value_parser: Callable[[dict], Any] = None,
                 value_validator: Callable[[dict], bool] = None,
                 value_converter: Callable[[dict], Any] = None):
        """
        Initialize a field.

        Args:
            name: The column name when saving to backend database, or showing in dataframe.
            raw_name: The raw name of that field defined in input record (object converted from Json, etc.).
                      It is used by default parser to capture value from input record. Nested objects can
                      be expressed as "obj1.obj2.attr" for default parser. None will be returned by the default
                      parser if the "raw_name" cannot match any attribute in the record object. If "raw_name" is
                      set to None, the value of "name" will be used.
            value_parser: A callable to parse the value of this field on a given record object. If None is set,
                          a default setter method will be used, to automatically match the data with raw_name
                          (see "raw_name").
            default: Specify the default value for that field if missing.
            value_converter: Specify a method for preprocessing when collecting the raw data on that field.
            value_validator: Specify a method that validates the data.
        """
        self._name = name
        self._default_value = default
        self._raw_name = raw_name if raw_name is not None else name
        self._value_converter = value_converter
        self._value_validator = value_validator
        self._value_parser = value_parser if value_parser is not None else self.default_value_parser
        self._type = None
        self._splited_key_names = self.raw_name.split(".")

    @property
    def name(self):
        return self._name

    @property
    def raw_name(self):
        return self._raw_name

    @property
    def default_value(self):
        """ The default value of this field. """
        return self._default_value

    @property
    def type(self):
        """ The SQLite type of this field """
        return "BLOB"

    def parse(self, record):
        """
        Parse value of this field from given record.

        Args:
            record: The record to be parsed.
        """
        return self._value_parser(record)

    def default_value_parser(self, record):
        """ Default value parser """
        r = record
        for key in self._splited_key_names:
            if isinstance(r, dict) and key in r.keys():
                r = r[key]
            else:
                return None
        if self._value_validator is not None and self._value_validator(r) is False:
            raise ValueError(f"Value error when parsing field {self.name} according to configured validator: {str(r)}")
        if self._value_converter is not None:
            r = self._value_converter(r)
        return r


class String(Field):
    """
    Represents a string field
    """

    @property
    def type(self):
        return "TEXT"

    def default_value_parser(self, record):
        record = super().default_value_parser(record)
        return str(record) if record is not None else None


class Int(Field):
    """
    Represents a field that stores an integer.
    """

    @property
    def type(self):
        return "INTEGER"

    def default_value_parser(self, record):
        record = super().default_value_parser(record)
        return int(record) if record is not None else None


class Float(Field):
    """
    Field that stores a float value.
    """

    @property
    def type(self):
        return "REAL"

    def default_value_parser(self, record):
        record = super().default_value_parser(record)
        return float(record) if record is not None else float('nan')


class DateTime(Field):
    """
    Field that stores a time.datetime.datetime value.
    """

    @property
    def type(self):
        return "INTEGER"
