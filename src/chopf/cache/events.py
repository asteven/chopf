import dataclasses


class Event:
#    def __getattr__(self, which):
#        """Make subclasses available in the class namespace.
#        Allows to use patterns like the following without having
#        to import all the event classes.
#
#        ```
#        match type(event):
#            case event.CreateEvent:
#                pass
#            case event.UpdateEvent:
#                pass
#        ```
#        """
#        for subclass in Event.__subclasses__():
#            if which == subclass.__name__:
#                return subclass

    def __init_subclass__(cls, **kwargs):
        """Make subclasses available in the class namespace.
        Allows to use patterns like the following without having
        to import all the event classes.

        ```
        match type(event):
            case event.CreateEvent:
                pass
            case event.UpdateEvent:
                pass
        ```
        """
        setattr(Event, cls.__name__, cls)

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.obj}>'


@dataclasses.dataclass(repr=False)
class CreateEvent(Event):
    obj: object


@dataclasses.dataclass(repr=False)
class UpdateEvent(Event):
    old: object
    new: object

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.old} {self.new}>'


@dataclasses.dataclass(repr=False)
class DeleteEvent(Event):
    obj: object


@dataclasses.dataclass(repr=False)
class GenericEvent(Event):
    obj: object
