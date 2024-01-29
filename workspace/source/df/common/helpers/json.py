from itertools import chain, starmap, product


def denormalize(x):
    """ Денормализует структуру JSON, разворачивая ее построчно
        (избавляемся от списков)
    """
    if isinstance(x, dict):
        keys = x.keys()
        values = (denormalize(i) for i in x.values())
        for i in product(*values):
            yield (dict(zip(keys, i)))
    elif isinstance(x, list):
        if not x:
            yield None
        for i in x:
            yield from denormalize(i)
    else:
        yield x


def flatten_record(dictionary, delimiter):
    """ Превращаем JSON в словарь """

    def unpack(parent_key, parent_value):
        """ Работа с одним уровнем вложенности """

        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                temp1 = parent_key + delimiter + key
                yield temp1, value
        elif isinstance(parent_value, list):
            i = 0
            for value in parent_value:
                temp2 = parent_key + delimiter + str(i)
                i += 1
                yield temp2, value
        else:
            yield parent_key, parent_value

    # Снимаем по одному уровню вложенности, пока не придем к словарю
    while True:
        dictionary = dict(chain.from_iterable(
            starmap(unpack, dictionary.items())))
        if not any(isinstance(value, dict) for value in dictionary.values()) and \
                not any(isinstance(value, list) for value in dictionary.values()):
            break

    return dictionary
