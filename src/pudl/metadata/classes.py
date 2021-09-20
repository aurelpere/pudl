"""Metadata data classes."""
import copy
import datetime
import os
from pathlib import Path
import re
from typing import (Any, Callable, Dict, Iterable, List, Literal, Optional,
                    Tuple, Type, Union)

import jinja2
import pandas as pd
import pydantic
import sqlalchemy as sa

from .constants import (CONSTRAINT_DTYPES, CONTRIBUTORS,
                        CONTRIBUTORS_BY_SOURCE, FIELD_DTYPES, FIELD_DTYPES_SQL,
                        KEYWORDS_BY_SOURCE, LICENSES, PERIODS, SOURCES)
from .fields import FIELD_METADATA
from .helpers import (expand_periodic_column_names, groupby_aggregate,
                      most_and_more_frequent, split_period)
from .resources import FOREIGN_KEYS, RESOURCE_METADATA

# ---- Helpers ---- #


def _unique(*args: Iterable) -> list:
    """
    Return a list of all unique values, in order of first appearance.

    Args:
        args: Iterables of values.

    Examples:
        >>> _unique([0, 2], (2, 1))
        [0, 2, 1]
        >>> _unique([{'x': 0, 'y': 1}, {'y': 1, 'x': 0}], [{'z': 2}])
        [{'x': 0, 'y': 1}, {'z': 2}]
    """
    values = []
    for parent in args:
        for child in parent:
            if child not in values:
                values.append(child)
    return values


def _format_pydantic_errors(*errors: str, header: bool = False) -> str:
    """
    Format multiple validation errors into a single error for pydantic.

    Args:
        errors: Error messages.
        header: Whether first error message should be treated as a header.

    Examples:
        >>> e = _format_pydantic_errors('Header:', 'bad', 'worse', header=True)
        >>> print(e)
        Header:
          * bad
          * worse
        >>> e = _format_pydantic_errors('Header:', 'bad', 'worse')
        >>> print(e)
        * Header:
          * bad
          * worse
    """
    if not errors:
        return ""
    return (
        f"{'' if header else '* '}{errors[0]}\n" +
        "\n".join([f"  * {e}" for e in errors[1:]])
    )


def _format_for_sql(x: Any, identifier: bool = False) -> str:  # noqa: C901
    """
    Format value for use in raw SQL(ite).

    Args:
        x: Value to format.
        identifier: Whether `x` represents an identifier
            (e.g. table, column) name.

    Examples:
        >>> _format_for_sql('table_name', identifier=True)
        '"table_name"'
        >>> _format_for_sql('any string')
        "'any string'"
        >>> _format_for_sql("Single's quote")
        "'Single''s quote'"
        >>> _format_for_sql(None)
        'null'
        >>> _format_for_sql(1)
        '1'
        >>> _format_for_sql(True)
        'True'
        >>> _format_for_sql(False)
        'False'
        >>> _format_for_sql(re.compile("^[^']*$"))
        "'^[^'']*$'"
        >>> _format_for_sql(datetime.date(2020, 1, 2))
        "'2020-01-02'"
        >>> _format_for_sql(datetime.datetime(2020, 1, 2, 3, 4, 5, 6))
        "'2020-01-02 03:04:05'"
    """
    if identifier:
        if isinstance(x, str):
            # Table and column names are escaped with double quotes (")
            return f'"{x}"'
        raise ValueError("Identifier must be a string")
    if x is None:
        return "null"
    elif isinstance(x, (int, float)):
        # NOTE: nan and (-)inf are TEXT in sqlite but numeric in postgresSQL
        return str(x)
    elif x is True:
        return "TRUE"
    elif x is False:
        return "FALSE"
    elif isinstance(x, re.Pattern):
        x = x.pattern
    elif isinstance(x, datetime.datetime):
        # Check datetime.datetime first, since also datetime.date
        x = x.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(x, datetime.date):
        x = x.strftime("%Y-%m-%d")
    if not isinstance(x, str):
        raise ValueError(f"Cannot format type {type(x)} for SQL")
    # Single quotes (') are escaped by doubling them ('')
    x = x.replace("'", "''")
    return f"'{x}'"


JINJA_ENVIRONMENT: jinja2.Environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "templates")
    ),
    autoescape=True
)


# ---- Base ---- #


class Base(pydantic.BaseModel):
    """
    Custom Pydantic base class.

    It overrides :meth:`fields` and :meth:`schema` to allow properties with those names.
    To use them in a class, use an underscore prefix and an alias.

    Examples:
        >>> class Class(Base):
        ...     fields_: List[str] = pydantic.Field(alias="fields")
        >>> m = Class(fields=['x'])
        >>> m
        Class(fields=['x'])
        >>> m.fields
        ['x']
        >>> m.fields = ['y']
        >>> m.dict()
        {'fields': ['y']}
    """

    class Config:
        """Custom Pydantic configuration."""

        validate_all: bool = True
        validate_assignment: bool = True
        extra: str = 'forbid'

    def dict(self, *args, by_alias=True, **kwargs) -> dict:  # noqa: A003
        """Return as a dictionary."""
        return super().dict(*args, by_alias=by_alias, **kwargs)

    def json(self, *args, by_alias=True, **kwargs) -> str:
        """Return as JSON."""
        return super().json(*args, by_alias=by_alias, **kwargs)

    def __getattribute__(self, name: str) -> Any:
        """Get attribute."""
        if name in ("fields", "schema") and f"{name}_" in self.__dict__:
            name = f"{name}_"
        return super().__getattribute__(name)

    def __setattr__(self, name, value) -> None:
        """Set attribute."""
        if name in ("fields", "schema") and f"{name}_" in self.__dict__:
            name = f"{name}_"
        super().__setattr__(name, value)

    def __repr_args__(self) -> List[Tuple[str, Any]]:
        """Returns the attributes to show in __str__, __repr__, and __pretty__."""
        return [
            (a[:-1] if a in ("fields_", "schema_") else a, v)
            for a, v in self.__dict__.items()
        ]


# ---- Class attribute types ---- #

# NOTE: Using regex=r"^\S(.*\S)*$" to fail on whitespace is too slow
String = pydantic.constr(min_length=1, strict=True, regex=r"^\S+(\s+\S+)*$")
"""
Non-empty :class:`str` with no trailing or leading whitespace.
"""

SnakeCase = pydantic.constr(
    min_length=1, strict=True, regex=r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$"
)
"""Snake-case variable name :class:`str` (e.g. 'pudl', 'entity_eia860')."""

Bool = pydantic.StrictBool
"""
Any :class:`bool` (`True` or `False`).
"""

Float = pydantic.StrictFloat
"""
Any :class:`float`.
"""

Int = pydantic.StrictInt
"""
Any :class:`int`.
"""

PositiveInt = pydantic.conint(ge=0, strict=True)
"""
Positive :class:`int`.
"""

PositiveFloat = pydantic.confloat(ge=0, strict=True)
"""
Positive :class:`float`.
"""


class Date:
    """Any :class:`datetime.date`."""

    @classmethod
    def __get_validators__(cls) -> Callable:
        """Yield validator methods."""
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> datetime.date:
        """Validate as date."""
        if not isinstance(value, datetime.date):
            raise TypeError("value is not a date")
        return value


class Datetime:
    """Any :class:`datetime.datetime`."""

    @classmethod
    def __get_validators__(cls) -> Callable:
        """Yield validator methods."""
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> datetime.datetime:
        """Validate as datetime."""
        if not isinstance(value, datetime.datetime):
            raise TypeError("value is not a datetime")
        return value


class Pattern:
    """Regular expression pattern."""

    @classmethod
    def __get_validators__(cls) -> Callable:
        """Yield validator methods."""
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> re.Pattern:
        """Validate as pattern."""
        if not isinstance(value, (str, re.Pattern)):
            raise TypeError("value is not a string or compiled regular expression")
        if isinstance(value, str):
            try:
                value = re.compile(value)
            except re.error:
                raise ValueError("string is not a valid regular expression")
        return value


def StrictList(item_type: Type = Any) -> pydantic.ConstrainedList:  # noqa: N802
    """
    Non-empty :class:`list`.

    Allows :class:`list`, :class:`tuple`, :class:`set`, :class:`frozenset`,
    :class:`collections.deque`, or generators and casts to a :class:`list`.
    """
    return pydantic.conlist(item_type=item_type, min_items=1)


# ---- Class attribute validators ---- #


def _check_unique(value: list = None) -> Optional[list]:
    """Check that input list has unique values."""
    if value:
        for i in range(len(value)):
            if value[i] in value[:i]:
                raise ValueError(f"contains duplicate {value[i]}")
    return value


def _stringify(value: Any = None) -> Optional[str]:
    """Convert input to string."""
    if value:
        return str(value)
    return value


def _validator(*names, fn: Callable) -> Callable:
    """
    Construct reusable Pydantic validator.

    Args:
        names: Names of attributes to validate.
        fn: Validation function (see :meth:`pydantic.validator`).

    Examples:
        >>> class Class(Base):
        ...     x: int = None
        ...     y: list = None
        ...     _stringify = _validator("x", fn=_stringify)
        ...     _check_unique = _validator("y", fn=_check_unique)
        >>> Class(x=1).x
        '1'
        >>> Class(y=[0, 0])
        Traceback (most recent call last):
        ValidationError: ...
    """
    return pydantic.validator(*names, allow_reuse=True)(fn)


# ---- Classes: Field ---- #


class FieldConstraints(Base):
    """
    Field constraints (`resource.schema.fields[...].constraints`).

    See https://specs.frictionlessdata.io/table-schema/#constraints.
    """

    required: Bool = False
    unique: Bool = False
    min_length: PositiveInt = None
    max_length: PositiveInt = None
    minimum: Union[Int, Float, Date, Datetime] = None
    maximum: Union[Int, Float, Date, Datetime] = None
    pattern: Pattern = None
    # TODO: Replace with String (min_length=1) once "" removed from enums
    enum: StrictList(Union[pydantic.StrictStr, Int, Float, Bool, Date, Datetime]) = None

    _check_unique = _validator("enum", fn=_check_unique)

    @pydantic.validator("max_length")
    def _check_max_length(cls, value, values):  # noqa: N805
        minimum, maximum = values.get("min_length"), value
        if minimum is not None and maximum is not None:
            if type(minimum) is not type(maximum):
                raise ValueError("must be same type as min_length")
            if maximum < minimum:
                raise ValueError("must be greater or equal to min_length")
        return value

    @pydantic.validator("maximum")
    def _check_max(cls, value, values):  # noqa: N805
        minimum, maximum = values.get("minimum"), value
        if minimum is not None and maximum is not None:
            if type(minimum) is not type(maximum):
                raise ValueError("must be same type as minimum")
            if maximum < minimum:
                raise ValueError("must be greater or equal to minimum")
        return value


class FieldHarvest(Base):
    """Field harvest parameters (`resource.schema.fields[...].harvest`)."""

    # NOTE: Callables with defaults must use pydantic.Field() to not bind to self
    aggregate: Callable[[pd.Series], pd.Series] = pydantic.Field(
        default=lambda x: most_and_more_frequent(x, min_frequency=0.7)
    )
    """Computes a single value from all field values in a group."""

    tolerance: PositiveFloat = 0.0
    """Fraction of invalid groups above which result is considered invalid."""


class Field(Base):
    """
    Field (`resource.schema.fields[...]`).

    See https://specs.frictionlessdata.io/table-schema/#field-descriptors.

    Examples:
        >>> field = Field(name='x', type='string', constraints={'enum': ['x', 'y']})
        >>> field.dtype
        CategoricalDtype(categories=['x', 'y'], ordered=False)
        >>> field.to_sql()
        Column('x', Enum('x', 'y'), CheckConstraint(...), table=None)
        >>> field = Field.from_id('utility_id_eia')
        >>> field.name
        'utility_id_eia'
    """

    name: SnakeCase
    type: String  # noqa: A003
    format: Literal["default"] = "default"  # noqa: A003
    description: String = None
    constraints: FieldConstraints = {}
    harvest: FieldHarvest = {}

    @pydantic.validator("type")
    # NOTE: Could be replaced with `type: Literal[...]`
    def _check_type_supported(cls, value):  # noqa: N805
        if value not in FIELD_DTYPES:
            raise ValueError(f"must be one of {list(FIELD_DTYPES.keys())}")
        return value

    @pydantic.validator("constraints")
    def _check_constraints(cls, value, values):  # noqa: N805, C901
        if "type" not in values:
            return value
        dtype = values["type"]
        errors = []
        for key in ("min_length", "max_length", "pattern"):
            if getattr(value, key) is not None and dtype != "string":
                errors.append(f"{key} not supported by {dtype} field")
        for key in ("minimum", "maximum"):
            x = getattr(value, key)
            if x is not None:
                if dtype in ("string", "boolean"):
                    errors.append(f"{key} not supported by {dtype} field")
                elif not isinstance(x, CONSTRAINT_DTYPES[dtype]):
                    errors.append(f"{key} not {dtype}")
        if value.enum:
            for x in value.enum:
                if not isinstance(x, CONSTRAINT_DTYPES[dtype]):
                    errors.append(f"enum value {x} not {dtype}")
        if errors:
            raise ValueError(_format_pydantic_errors(*errors))
        return value

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Construct dictionary from PUDL identifier (`Field.name`)."""
        return {'name': x, **copy.deepcopy(FIELD_METADATA[x])}

    @classmethod
    def from_id(cls, x: str) -> 'Field':
        """Construct from PUDL identifier (`Field.name`)."""
        return cls(**cls.dict_from_id(x))

    @property
    def dtype(self) -> Union[str, pd.CategoricalDtype]:
        """Pandas data type."""
        if self.constraints.enum:
            return pd.CategoricalDtype(self.constraints.enum)
        return FIELD_DTYPES[self.type]

    @property
    def dtype_sql(self) -> sa.sql.visitors.VisitableType:
        """SQLAlchemy data type."""  # noqa: D403
        if self.constraints.enum and self.type == "string":
            return sa.Enum(*self.constraints.enum)
        return FIELD_DTYPES_SQL[self.type]

    def to_sql(  # noqa: C901
        self,
        dialect: Literal["sqlite"] = "sqlite",
        check_types: bool = True,
        check_values: bool = True,
    ) -> sa.Column:
        """Return equivalent SQL column."""
        if dialect != "sqlite":
            raise NotImplementedError(f"Dialect {dialect} is not supported")
        checks = []
        name = _format_for_sql(self.name, identifier=True)
        if check_types:
            # Required with TYPEOF since TYPEOF(NULL) = 'null'
            prefix = "" if self.constraints.required else f"{name} IS NULL OR "
            # Field type
            if self.type == "string":
                checks.append(f"{prefix}TYPEOF({name}) = 'text'")
            elif self.type in ("integer", "year"):
                checks.append(f"{prefix}TYPEOF({name}) = 'integer'")
            elif self.type == "number":
                checks.append(f"{prefix}TYPEOF({name}) = 'real'")
            elif self.type == "boolean":
                # Just IN (0, 1) accepts floats equal to 0, 1 (0.0, 1.0)
                checks.append(
                    f"{prefix}(TYPEOF({name}) = 'integer' AND {name} IN (0, 1))")
            elif self.type == "date":
                checks.append(f"{name} IS DATE({name})")
            elif self.type == "datetime":
                checks.append(f"{name} IS DATETIME({name})")
        if check_values:
            # Field constraints
            if self.constraints.min_length is not None:
                checks.append(f"LENGTH({name}) >= {self.constraints.min_length}")
            if self.constraints.max_length is not None:
                checks.append(f"LENGTH({name}) <= {self.constraints.max_length}")
            if self.constraints.minimum is not None:
                minimum = _format_for_sql(self.constraints.minimum)
                checks.append(f"{name} >= {minimum}")
            if self.constraints.maximum is not None:
                maximum = _format_for_sql(self.constraints.maximum)
                checks.append(f"{name} <= {maximum}")
            if self.constraints.pattern:
                pattern = _format_for_sql(self.constraints.pattern)
                checks.append(f"{name} REGEXP {pattern}")
            if self.constraints.enum:
                enum = [_format_for_sql(x) for x in self.constraints.enum]
                checks.append(f"{name} IN ({', '.join(enum)})")
        return sa.Column(
            self.name,
            self.dtype_sql,
            *[sa.CheckConstraint(check) for check in checks],
            nullable=not self.constraints.required,
            unique=self.constraints.unique,
            comment=self.description
        )


# ---- Classes: Resource ---- #


class ForeignKeyReference(Base):
    """
    Foreign key reference (`resource.schema.foreign_keys[...].reference`).

    See https://specs.frictionlessdata.io/table-schema/#foreign-keys.
    """

    resource: SnakeCase
    fields_: StrictList(SnakeCase) = pydantic.Field(alias="fields")

    _check_unique = _validator("fields_", fn=_check_unique)


class ForeignKey(Base):
    """
    Foreign key (`resource.schema.foreign_keys[...]`).

    See https://specs.frictionlessdata.io/table-schema/#foreign-keys.
    """

    fields_: StrictList(SnakeCase) = pydantic.Field(alias="fields")
    reference: ForeignKeyReference

    _check_unique = _validator("fields_", fn=_check_unique)

    @pydantic.validator("reference")
    def _check_fields_equal_length(cls, value, values):  # noqa: N805
        if "fields_" in values:
            if len(value.fields) != len(values["fields_"]):
                raise ValueError("fields and reference.fields are not equal length")
        return value

    def to_sql(self) -> sa.ForeignKeyConstraint:
        """Return equivalent SQL Foreign Key."""
        return sa.ForeignKeyConstraint(
            self.fields,
            [f"{self.reference.resource}.{field}" for field in self.reference.fields]
        )


class Schema(Base):
    """
    Table schema (`resource.schema`).

    See https://specs.frictionlessdata.io/table-schema.
    """

    fields_: StrictList(Field) = pydantic.Field(alias="fields")
    missing_values: List[pydantic.StrictStr] = [""]
    primary_key: StrictList(SnakeCase) = None
    foreign_keys: List[ForeignKey] = []

    _check_unique = _validator(
        "missing_values", "primary_key", "foreign_keys", fn=_check_unique
    )

    @pydantic.validator("fields_")
    def _check_field_names_unique(cls, value):  # noqa: N805
        _check_unique([f.name for f in value])
        return value

    @pydantic.validator("primary_key")
    def _check_primary_key_in_fields(cls, value, values):  # noqa: N805
        if value is not None and "fields_" in values:
            missing = []
            names = [f.name for f in values['fields_']]
            for name in value:
                if name in names:
                    # Flag primary key fields as required
                    field = values['fields_'][names.index(name)]
                    field.constraints.required = True
                else:
                    missing.append(field.name)
            if missing:
                raise ValueError(f"names {missing} missing from fields")
        return value

    @pydantic.validator("foreign_keys", each_item=True)
    def _check_foreign_key_in_fields(cls, value, values):  # noqa: N805
        if value and "fields_" in values:
            names = [f.name for f in values['fields_']]
            missing = [x for x in value.fields if x not in names]
            if missing:
                raise ValueError(f"names {missing} missing from fields")
        return value


class License(Base):
    """
    Data license (`package|resource.licenses[...]`).

    See https://specs.frictionlessdata.io/data-package/#licenses.
    """

    name: String
    title: String
    path: pydantic.AnyHttpUrl

    _stringify = _validator("path", fn=_stringify)

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Construct dictionary from PUDL identifier."""
        return copy.deepcopy(LICENSES[x])

    @classmethod
    def from_id(cls, x: str) -> "License":
        """Construct from PUDL identifier."""
        return cls(**cls.dict_from_id(x))


class Source(Base):
    """
    Data source (`package|resource.sources[...]`).

    See https://specs.frictionlessdata.io/data-package/#sources.
    """

    title: String
    path: pydantic.AnyHttpUrl
    email: pydantic.EmailStr = None

    _stringify = _validator("path", "email", fn=_stringify)

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Construct dictionary from PUDL identifier."""
        return copy.deepcopy(SOURCES[x])

    @classmethod
    def from_id(cls, x: str) -> "Source":
        """Construct from PUDL identifier."""
        return cls(**cls.dict_from_id(x))


class Contributor(Base):
    """
    Data contributor (`package.contributors[...]`).

    See https://specs.frictionlessdata.io/data-package/#contributors.
    """

    title: String
    path: pydantic.AnyHttpUrl = None
    email: pydantic.EmailStr = None
    role: Literal[
        "author", "contributor", "maintainer", "publisher", "wrangler"
    ] = "contributor"
    organization: String = None

    _stringify = _validator("path", "email", fn=_stringify)

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Construct dictionary from PUDL identifier."""
        return copy.deepcopy(CONTRIBUTORS[x])

    @classmethod
    def from_id(cls, x: str) -> "Contributor":
        """Construct from PUDL identifier."""
        return cls(**cls.dict_from_id(x))


class ResourceHarvest(Base):
    """Resource harvest parameters (`resource.harvest`)."""

    harvest: Bool = False
    """
    Whether to harvest from dataframes based on field names.

    If `False`, the dataframe with the same name is used
    and the process is limited to dropping unwanted fields.
    """

    tolerance: PositiveFloat = 0.0
    """Fraction of invalid fields above which result is considerd invalid."""


class Resource(Base):
    """
    Tabular data resource (`package.resources[...]`).

    See https://specs.frictionlessdata.io/tabular-data-resource.

    Examples:
        A simple example illustrates the conversion to SQLAlchemy objects.

        >>> fields = [{'name': 'x', 'type': 'year'}, {'name': 'y', 'type': 'string'}]
        >>> fkeys = [{'fields': ['x', 'y'], 'reference': {'resource': 'b', 'fields': ['x', 'y']}}]
        >>> schema = {'fields': fields, 'primary_key': ['x'], 'foreign_keys': fkeys}
        >>> resource = Resource(name='a', schema=schema)
        >>> table = resource.to_sql()
        >>> table.columns.x
        Column('x', Integer(), ForeignKey('b.x'), CheckConstraint(...), table=<a>, primary_key=True, nullable=False)
        >>> table.columns.y
        Column('y', Text(), ForeignKey('b.y'), CheckConstraint(...), table=<a>)

        To illustrate harvesting operations,
        say we have a resource with two fields - a primary key (`id`) and a data field -
        which we want to harvest from two different dataframes.

        >>> from pudl.metadata.helpers import unique, as_dict
        >>> fields = [
        ...     {'name': 'id', 'type': 'integer'},
        ...     {'name': 'x', 'type': 'integer', 'harvest': {'aggregate': unique, 'tolerance': 0.25}}
        ... ]
        >>> resource = Resource(**{
        ...     'name': 'a',
        ...     'harvest': {'harvest': True},
        ...     'schema': {'fields': fields, 'primary_key': ['id']}
        ... })
        >>> dfs = {
        ...     'a': pd.DataFrame({'id': [1, 1, 2, 2], 'x': [1, 1, 2, 2]}),
        ...     'b': pd.DataFrame({'id': [2, 3, 3], 'x': [3, 4, 4]})
        ... }

        Skip aggregation to access all the rows concatenated from the input dataframes.
        The names of the input dataframes are used as the index.

        >>> df, _ = resource.harvest_dfs(dfs, aggregate=False)
        >>> df
            id  x
        df
        a    1  1
        a    1  1
        a    2  2
        a    2  2
        b    2  3
        b    3  4
        b    3  4

        Field names and data types are enforced.

        >>> resource.dtypes == df.dtypes.apply(str).to_dict()
        True

        Alternatively, aggregate by primary key
        (the default when :attr:`harvest`. `harvest=True`)
        and report aggregation errors.

        >>> df, report = resource.harvest_dfs(dfs)
        >>> df
               x
        id
        1      1
        2   <NA>
        3      4
        >>> report['stats']
        {'all': 2, 'invalid': 1, 'tolerance': 0.0, 'actual': 0.5}
        >>> report['fields']['x']['stats']
        {'all': 3, 'invalid': 1, 'tolerance': 0.25, 'actual': 0.33...}
        >>> report['fields']['x']['errors']
        id
        2    Not unique.
        Name: x, dtype: object

        Customize the error values in the error report.

        >>> error = lambda x, e: as_dict(x)
        >>> df, report = resource.harvest_dfs(dfs, raised=False, error=error)
        >>> report['fields']['x']['errors']
        id
        2    {'a': [2, 2], 'b': [3]}
        Name: x, dtype: object

        Limit harvesting to the input dataframe of the same name
        by setting :attr:`harvest`. `harvest=False`.

        >>> resource.harvest.harvest = False
        >>> df, _ = resource.harvest_dfs(dfs, raised=False)
        >>> df
            id  x
        df
        a    1  1
        a    1  1
        a    2  2
        a    2  2

        Harvesting can also handle conversion to longer time periods.
        Period harvesting requires primary key fields with a `datetime` data type,
        except for `year` fields which can be integer.

        >>> fields = [{'name': 'report_year', 'type': 'year'}]
        >>> resource = Resource(**{
        ...     'name': 'table', 'harvest': {'harvest': True},
        ...     'schema': {'fields': fields, 'primary_key': ['report_year']}
        ... })
        >>> df = pd.DataFrame({'report_date': ['2000-02-02', '2000-03-03']})
        >>> resource.format_df(df)
          report_year
        0  2000-01-01
        1  2000-01-01
        >>> df = pd.DataFrame({'report_year': [2000, 2000]})
        >>> resource.format_df(df)
          report_year
        0  2000-01-01
        1  2000-01-01
    """

    name: SnakeCase
    title: String = None
    description: String = None
    harvest: ResourceHarvest = {}
    schema_: Schema = pydantic.Field(alias='schema')
    contributors: List[Contributor] = []
    licenses: List[License] = []
    sources: List[Source] = []
    keywords: List[String] = []

    _check_unique = _validator(
        "contributors", "keywords", "licenses", "sources", fn=_check_unique
    )

    @pydantic.validator("schema_")
    def _check_harvest_primary_key(cls, value, values):  # noqa: N805
        if values["harvest"].harvest:
            if not value.primary_key:
                raise ValueError("Harvesting requires a primary key")
        return value

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """
        Construct dictionary from PUDL identifier (`resource.name`).

        * `schema.fields`

          * Field names are expanded (:meth:`Field.from_id`).
          * Field descriptors are expanded by name
            (e.g. `{'name': 'x', ...}` to `Field.from_id('x')`)
            and updated with custom properties (e.g. `{..., 'description': '...'}`).

        * `sources`

          * Source ids are expanded (:meth:`Source.from_id`).
          * Source descriptors are used as is.

        * `contributors`: Contributor ids are fetched by source ids,
          then expanded (:meth:`Contributor.from_id`).
        * `keywords`: Keywords are fetched by source ids.
        * `schema.foreign_keys`: Foreign keys are fetched by resource name.
        """
        obj = copy.deepcopy(RESOURCE_METADATA[x])
        obj["name"] = x
        schema = obj["schema"]
        # Expand fields
        if "fields" in schema:
            fields = []
            for value in schema["fields"]:
                if isinstance(value, str):
                    # Lookup field by name
                    fields.append(Field.dict_from_id(value))
                else:
                    # Lookup field by name and update with custom metadata
                    fields.append({**Field.dict_from_id(value["name"]), **value})
            schema["fields"] = fields
        # Expand sources
        sources = obj.get("sources", [])
        obj["sources"] = [
            Source.dict_from_id(value) if isinstance(value, str) else value
            for value in sources
        ]
        # Expand licenses (assign CC-BY-4.0 by default)
        licenses = obj.get("licenses", [License.dict_from_id("cc-by-4.0")])
        obj["licenses"] = [
            License.dict_from_id(value) if isinstance(value, str) else value
            for value in licenses
        ]
        # Lookup and insert contributors
        if "contributors" in schema:
            raise ValueError("Resource metadata contains explicit contributors")
        cids = []
        for source in sources:
            cids.extend(CONTRIBUTORS_BY_SOURCE.get(source, []))
        obj["contributors"] = [Contributor.dict_from_id(cid) for cid in set(cids)]
        # Lookup and insert keywords
        if "keywords" in schema:
            raise ValueError("Resource metadata contains explicit keywords")
        keywords = []
        for source in sources:
            keywords.extend(KEYWORDS_BY_SOURCE.get(source, []))
        obj["keywords"] = list(set(keywords))
        # Insert foreign keys
        if "foreign_keys" in schema:
            raise ValueError("Resource metadata contains explicit foreign keys")
        schema["foreign_keys"] = FOREIGN_KEYS.get(x, [])
        # Delete foreign key rules
        if "foreign_key_rules" in schema:
            del schema["foreign_key_rules"]
        return obj

    @classmethod
    def from_id(cls, x: str) -> "Resource":
        """Construct from PUDL identifier (`resource.name`)."""
        return cls(**cls.dict_from_id(x))

    def to_sql(
        self,
        metadata: sa.MetaData = None,
        check_types: bool = True,
        check_values: bool = True,
    ) -> sa.Table:
        """Return equivalent SQL Table."""
        if metadata is None:
            metadata = sa.MetaData()
        columns = [
            f.to_sql(
                check_types=check_types,
                check_values=check_values,
            )
            for f in self.schema.fields
        ]
        constraints = []
        if self.schema.primary_key:
            constraints.append(sa.PrimaryKeyConstraint(*self.schema.primary_key))
        for key in self.schema.foreign_keys:
            constraints.append(key.to_sql())
        return sa.Table(self.name, metadata, *columns, *constraints)

    @property
    def dtypes(self) -> Dict[str, Union[str, pd.CategoricalDtype]]:
        """Pandas data type of each field by field name."""
        return {f.name: f.dtype for f in self.schema.fields}

    def match_primary_key(self, names: Iterable[str]) -> Optional[Dict[str, str]]:
        """
        Match primary key fields to input field names.

        An exact match is required unless :attr:`harvest` .`harvest=True`,
        in which case periodic names may also match a basename with a smaller period.

        Args:
            names: Field names.

        Raises:
            ValueError: Field names are not unique.
            ValueError: Multiple field names match primary key field.

        Returns:
            The name matching each primary key field (if any) as a :class:`dict`,
            or `None` if not all primary key fields have a match.

        Examples:
            >>> fields = [{'name': 'x_year', 'type': 'year'}]
            >>> schema = {'fields': fields, 'primary_key': ['x_year']}
            >>> resource = Resource(name='r', schema=schema)

            By default, when :attr:`harvest` .`harvest=False`,
            exact matches are required.

            >>> resource.harvest.harvest
            False
            >>> resource.match_primary_key(['x_month']) is None
            True
            >>> resource.match_primary_key(['x_year', 'x_month'])
            {'x_year': 'x_year'}

            When :attr:`harvest` .`harvest=True`,
            in the absence of an exact match,
            periodic names may also match a basename with a smaller period.

            >>> resource.harvest.harvest = True
            >>> resource.match_primary_key(['x_year', 'x_month'])
            {'x_year': 'x_year'}
            >>> resource.match_primary_key(['x_month'])
            {'x_month': 'x_year'}
            >>> resource.match_primary_key(['x_month', 'x_date'])
            Traceback (most recent call last):
            ValueError: ... {'x_month', 'x_date'} match primary key field 'x_year'
        """
        if len(names) != len(set(names)):
            raise ValueError("Field names are not unique")
        keys = self.schema.primary_key or []
        if self.harvest.harvest:
            remaining = set(names)
            matches = {}
            for key in keys:
                match = None
                if key in remaining:
                    # Use exact match if present
                    match = key
                elif split_period(key)[1]:
                    # Try periodic alternatives
                    periods = expand_periodic_column_names([key])
                    matching = remaining.intersection(periods)
                    if len(matching) > 1:
                        raise ValueError(
                            f"Multiple field names {matching} "
                            f"match primary key field '{key}'"
                        )
                    if len(matching) == 1:
                        match = list(matching)[0]
                if match:
                    matches[match] = key
                    remaining.remove(match)
        else:
            matches = {key: key for key in keys if key in names}
        return matches if len(matches) == len(keys) else None

    def format_df(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Format a dataframe.

        Args:
            df: Dataframe to format.

        Returns:
            Dataframe with column names and data types matching the resource fields.
            Periodic primary key fields are snapped to the start of the desired period.
            If the primary key fields could not be matched to columns in `df`
            (:meth:`match_primary_key`) or if `df=None`, an empty dataframe is returned.
        """
        if df is None:
            return pd.DataFrame({n: pd.Series(dtype=d) for n, d in self.dtypes.items()})
        matches = self.match_primary_key(df.columns)
        if matches is None:
            # Primary key present but no matches were found
            return self.format_df()
        df = df.copy()
        # Rename periodic key columns (if any) to the requested period
        df.rename(columns=matches, inplace=True)
        # Cast integer year fields to datetime
        for field in self.schema.fields:
            if (
                field.type == "year" and
                field.name in df and
                pd.api.types.is_integer_dtype(df[field.name])
            ):
                df[field.name] = pd.to_datetime(df[field.name], format="%Y")
        df = (
            # Reorder columns and insert missing columns
            df.reindex(columns=self.dtypes.keys(), copy=False)
            # Coerce columns to correct data type
            .astype(self.dtypes, copy=False)
        )
        # Convert periodic key columns to the requested period
        for df_key, key in matches.items():
            _, period = split_period(key)
            if period and df_key != key:
                df[key] = PERIODS[period](df[key])
        return df

    def aggregate_df(
        self, df: pd.DataFrame, raised: bool = False, error: Callable = None
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Aggregate dataframe by primary key.

        The dataframe is grouped by primary key fields
        and aggregated with the aggregate function of each field
        (:attr:`schema_`. `fields[*].harvest.aggregate`).

        The report is formatted as follows:

        * `valid` (bool): Whether resouce is valid.
        * `stats` (dict): Error statistics for resource fields.
        * `fields` (dict):

          * `<field_name>` (str)

            * `valid` (bool): Whether field is valid.
            * `stats` (dict): Error statistics for field groups.
            * `errors` (:class:`pandas.Series`): Error values indexed by primary key.

          * ...

        Each `stats` (dict) contains the following:

        * `all` (int): Number of entities (field or field group).
        * `invalid` (int): Invalid number of entities.
        * `tolerance` (float): Fraction of invalid entities below which
          parent entity is considered valid.
        * `actual` (float): Actual fraction of invalid entities.

        Args:
            df: Dataframe to aggregate. It is assumed to have column names and
              data types matching the resource fields.
            raised: Whether aggregation errors are raised or
               replaced with :obj:`np.nan` and returned in an error report.
            error: A function with signature `f(x, e) -> Any`,
              where `x` are the original field values as a :class:`pandas.Series`
              and `e` is the original error.
              If provided, the returned value is reported instead of `e`.

        Raises:
            ValueError: A primary key is required for aggregating.

        Returns:
            The aggregated dataframe indexed by primary key fields,
            and an aggregation report (descripted above)
            that includes all aggregation errors and whether the result
            meets the resource's and fields' tolerance.
        """
        if not self.schema.primary_key:
            raise ValueError("A primary key is required for aggregating")
        aggfuncs = {
            f.name: f.harvest.aggregate
            for f in self.schema.fields
            if f.name not in self.schema.primary_key
        }
        df, report = groupby_aggregate(
            df,
            by=self.schema.primary_key,
            aggfuncs=aggfuncs,
            raised=raised,
            error=error,
        )
        report = self._build_aggregation_report(df, report)
        return df, report

    def _build_aggregation_report(self, df: pd.DataFrame, errors: dict) -> dict:
        """
        Build report from aggregation errors.

        Args:
            df: Harvested dataframe (see :meth:`harvest_dfs`).
            errors: Aggregation errors (see :func:`groupby_aggregate`).

        Returns:
            Aggregation report, as described in :meth:`aggregate_df`.
        """
        nrows, ncols = df.reset_index().shape
        freports = {}
        for field in self.schema.fields:
            if field.name in errors:
                nerrors = errors[field.name].size
            else:
                nerrors = 0
            stats = {
                "all": nrows,
                "invalid": nerrors,
                "tolerance": field.harvest.tolerance,
                "actual": nerrors / nrows if nrows else 0,
            }
            freports[field.name] = {
                "valid": stats["actual"] <= stats["tolerance"],
                "stats": stats,
                "errors": errors.get(field.name, None),
            }
        nerrors = sum([not f["valid"] for f in freports.values()])
        stats = {
            "all": ncols,
            "invalid": nerrors,
            "tolerance": self.harvest.tolerance,
            "actual": nerrors / ncols,
        }
        return {
            "valid": stats["actual"] <= stats["tolerance"],
            "stats": stats,
            "fields": freports,
        }

    def harvest_dfs(
        self, dfs: Dict[str, pd.DataFrame], aggregate: bool = None, **kwargs: Any
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Harvest from named dataframes.

        For standard resources (:attr:`harvest`. `harvest=False`), the columns
        matching all primary key fields and any data fields are extracted from
        the input dataframe of the same name.

        For harvested resources (:attr:`harvest`. `harvest=True`), the columns
        matching all primary key fields and any data fields are extracted from
        each compatible input dataframe, and concatenated into a single
        dataframe.  Periodic key fields (e.g. 'report_month') are matched to any
        column of the same name with an equal or smaller period (e.g.
        'report_day') and snapped to the start of the desired period.

        If `aggregate=False`, rows are indexed by the name of the input dataframe.
        If `aggregate=True`, rows are indexed by primary key fields.

        Args:
            dfs: Dataframes to harvest.
            aggregate: Whether to aggregate the harvested rows by their primary key.
                By default, this is `True` if `self.harvest.harvest=True` and
                `False` otherwise.
            kwargs: Optional arguments to :meth:`aggregate_df`.

        Returns:
            A dataframe harvested from the dataframes, with column names and
            data types matching the resource fields, alongside an aggregation
            report.

        """
        if aggregate is None:
            aggregate = self.harvest.harvest
        if self.harvest.harvest:
            # Harvest resource from all inputs where all primary key fields are present
            samples = {}
            for name, df in dfs.items():
                samples[name] = self.format_df(df)
                # Pass input names to aggregate via the index
                samples[name].index = pd.Index([name] * len(samples[name]), name="df")
            df = pd.concat(samples.values())
        elif self.name in dfs:
            # Subset resource from input of same name
            df = self.format_df(dfs[self.name])
            # Pass input names to aggregate via the index
            df.index = pd.Index([self.name] * df.shape[0], name="df")
        else:
            return self.format_df(), {}
        if aggregate:
            return self.aggregate_df(df, **kwargs)
        return df, {}

    def to_rst(self, path: str) -> None:
        """Output to an RST file."""
        template = JINJA_ENVIRONMENT.get_template("resource.rst.jinja")
        rendered = template.render(resource=self)
        Path(path).write_text(rendered)


# ---- Package ---- #


class Package(Base):
    """
    Tabular data package.

    See https://specs.frictionlessdata.io/data-package.

    Examples:
        Foreign keys between resources are checked for completeness and consistency.

        >>> fields = [{'name': 'x', 'type': 'year'}, {'name': 'y', 'type': 'string'}]
        >>> fkey = {'fields': ['x', 'y'], 'reference': {'resource': 'b', 'fields': ['x', 'y']}}
        >>> schema = {'fields': fields, 'primary_key': ['x'], 'foreign_keys': [fkey]}
        >>> a = Resource(name='a', schema=schema)
        >>> b = Resource(name='b', schema=Schema(fields=fields, primary_key=['x']))
        >>> Package(name='ab', resources=[a, b])
        Traceback (most recent call last):
        ValidationError: ...
        >>> b.schema.primary_key = ['x', 'y']
        >>> package = Package(name='ab', resources=[a, b])

        SQL Alchemy can sort tables, based on foreign keys,
        in the order in which they need to be loaded into a database.

        >>> metadata = package.to_sql()
        >>> [table.name for table in metadata.sorted_tables]
        ['b', 'a']
    """

    name: String
    title: String = None
    description: String = None
    keywords: List[String] = []
    homepage: pydantic.HttpUrl = "https://catalyst.coop/pudl"
    created: Datetime = datetime.datetime.utcnow()
    contributors: List[Contributor] = []
    sources: List[Source] = []
    licenses: List[License] = []
    resources: StrictList(Resource)

    _stringify = _validator("homepage", fn=_stringify)

    @pydantic.validator("resources")
    def _check_foreign_keys(cls, value):  # noqa: N805
        rnames = [resource.name for resource in value]
        errors = []
        for resource in value:
            for foreign_key in resource.schema.foreign_keys:
                rname = foreign_key.reference.resource
                tag = f"[{resource.name} -> {rname}]"
                if rname not in rnames:
                    errors.append(f"{tag}: Reference not found")
                    continue
                reference = value[rnames.index(rname)]
                if not reference.schema.primary_key:
                    errors.append(f"{tag}: Reference missing primary key")
                    continue
                missing = [
                    x for x in foreign_key.reference.fields
                    if x not in reference.schema.primary_key
                ]
                if missing:
                    errors.append(f"{tag}: Reference primary key missing {missing}")
        if errors:
            raise ValueError(
                _format_pydantic_errors("Foreign keys", *errors, header=True)
            )
        return value

    @pydantic.root_validator(skip_on_failure=True)
    def _populate_from_resources(cls, values):  # noqa: N805
        for key in ('keywords', 'contributors', 'sources', 'licenses'):
            values[key] = _unique(
                values[key],
                *[getattr(r, key) for r in values['resources']]
            )
        return values

    @classmethod
    def from_resource_ids(
        cls, resource_ids: Iterable[str], resolve_foreign_keys: bool = False
    ) -> "Package":
        """
        Construct from PUDL identifiers (`resource.name`).

        Args:
            resource_ids: Resource PUDL identifiers (`resource.name`).
            resolve_foreign_keys: Whether to add resources as needed based on
                foreign keys.
        """
        resources = [Resource.dict_from_id(x) for x in resource_ids]
        if resolve_foreign_keys:
            # Add missing resources based on foreign keys
            names = list(resource_ids)
            i = 0
            while i < len(resources):
                for resource in resources[i:]:
                    for key in resource["schema"].get("foreign_keys", []):
                        name = key.get("reference", {}).get("resource")
                        if name and name not in names:
                            names.append(name)
                i = len(resources)
                if len(names) > i:
                    resources += [Resource.dict_from_id(x) for x in names[i:]]
        return cls(name="pudl", resources=resources)

    def to_rst(self, path: str) -> None:
        """Output to an RST file."""
        template = JINJA_ENVIRONMENT.get_template("package.rst.jinja")
        rendered = template.render(package=self)
        Path(path).write_text(rendered)

    def to_sql(
        self,
        check_types: bool = True,
        check_values: bool = True,
    ) -> sa.MetaData:
        """Return equivalent SQL MetaData."""
        metadata = sa.MetaData()
        for resource in self.resources:
            _ = resource.to_sql(
                metadata,
                check_types=check_types,
                check_values=check_values,
            )
        return metadata