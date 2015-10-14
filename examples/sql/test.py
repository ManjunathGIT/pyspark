mydict = {"key1": "value1", "key2": "value2"}


def test(**kwargs):
    names = sorted(kwargs.keys())
    values = tuple(kwargs[n] for n in names)

    print names
    print values


test(**mydict)
