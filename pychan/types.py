from decimal import Decimal

def _toTuple(argOrArgs):
    return argOrArgs if isinstance(argOrArgs, tuple) else (argOrArgs, )

def _genRepr(arg):
    if isinstance(arg, type):
        module = arg.__module__
        name = arg.__name__
        
        if module != "builtins":
            return f"{module}.{name}"
        return name
    
    return repr(arg)

class _Generic:
    __slots__ = ()
    expectedLength = 1
    
    @classmethod
    def __class_getitem__(cls, argOrArgs):
        args = _toTuple(argOrArgs)
        
        currentLength = len(args)
        if currentLength != cls.expectedLength:
            raise TypeError(f"{__name__}.{cls.__name__}: Length of arguments does not match.")
        
        repr = (
            f"{cls.__name__}"
            f"[{', '.join(_genRepr(arg) for arg in args)}]"
        )
        
        return type(repr, (cls, ), {"__args__": args})

class Base:
    def __repr__(self):
        return f"{self.__class__.__name__}()"
    
    @classmethod
    def typeName(cls):
        return cls.__name__.lower()

class Any(Base):
    pass

class Unit(Base):
    pass

class String(Base):
    """
    >>> organization: String = "SoftwareONE"
    """
    pass

class Boolean(Base):
    """
    >>> open: Boolean = True
    """
    pass

class Int(Base):
    """
    >>> period: Int = 1
    """
    pass

class Float(Base):
    pass

class Date(Base):
    """
    >>> verified: Date = datetime.date(2024, 9, 20)
    """
    pass

class Timestamp(Base):
    """
    >>> verified: Date = datetime.datetime(2024, 9, 20, 1, 1, 1, 100000)
    """
    pass

class List(Base, _Generic):
    """
    >>> about: List[String] = [
    ...     "SoftwareONE",
    ...     "SoftwareONE Vietnam"
    ... ]
    """
    pass

class Set(Base, _Generic):
    pass

class Optional(Base, _Generic):
    """
    >>> organization: Optional[String] = "SoftwareONE"
    """
    pass

class Map(Base, _Generic):
    """
    >>> about: Map[String, Any] = {
    ...     "organization": "SoftwareONE",
    ...     "verified": datetime.date(2024, 9, 16)
    ... }
    """
    expectedLength = 2
