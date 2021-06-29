from keyvaluestore.utils import resolve_sorted_iterable_duplicates

obj = [
    (1, 0),
    (1, 1),
    (1, 2),
    (2, 0),
    (2, 1),
    (2, 2),
    (2, 3),
    (3, 0),
    (4, 0),
    (5, 0),
    (5, 1),
    (6, 0),
    (7, 0),
    (7, 1),
    (7, 2),
    (7, 3),
    (7, 4),
    (7, 5),
    (7, 6),
    (7, 7),
    (7, 8),
    (7, 9),
    (7, 10),
    (7, 11),
    (7, 12),
    (7, 13),
    (7, 14),
    (7, 15),
    (7, 16),
    (7, 17),
    (7, 18),
    (7, 19),
    (8, 0),
]

def resolve_duplicate(xs):
    return min(xs, key=lambda x: abs(x[0] - x[1]))

def is_equal(x, y):
    return x[0] == y[0]

new_obj = list(resolve_sorted_iterable_duplicates(
    obj,
    resolve_duplicate,
    is_equal
))

print(new_obj)