import random
import string

SUPPORTED_TYPE = ['varchar', 'int', 'decimal']

def random_int(lower=-10000, upper=10000, unsigned=False):
    if unsigned:
        return random.randint(0, upper)
    else:
        return random.randint(lower, upper)

def random_varchar(length=10):
    return ''.join([random.choice(string.ascii_lowercase + ' ') \
                    for i in range(random.randint(1, length))])

def random_decimal(total_dig=8, right_dig=6, unsigned=False):
    if total_dig < right_dig:
        raise ValueError("Invalid decimal format decimal({}, {})".format(total_dig, right_dig))
    if right_dig == 0:
        right = 0
    else:
        right = random_int(lower=0, upper=10 ** (right_dig) - 1)
    if total_dig == right_dig:
        left_range = 0
    else:
        left_range = 10 ** (total_dig - right_dig) - 1
    if unsigned:
        left = random_int(lower=0, upper=left_range)
    else:
        left = random_int(lower=-left_range, upper=left_range)
    return float('.'.join([str(left), str(right)]))

function_map = {
    'varchar': random_varchar,
    'int': random_int,
    'decimal': random_decimal
}
