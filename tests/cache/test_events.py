from chopf.cache import events


def test_event_subclasses_registered_on_event_class():
    assert hasattr(events.Event, 'CreateEvent')
    assert hasattr(events.Event, 'UpdateEvent')
    assert hasattr(events.Event, 'DeleteEvent')


def test_event_repr_includes_object():
    obj = object()
    event = events.CreateEvent(obj=obj)
    assert repr(event) == f'<CreateEvent {obj}>'


def test_update_event_repr_lists_old_and_new():
    old = object()
    new = object()
    event = events.UpdateEvent(old=old, new=new)
    assert repr(event) == f'<UpdateEvent {old} {new}>'
