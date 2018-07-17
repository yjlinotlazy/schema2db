import random
import string
import requests
import re

# https://stackoverflow.com/questions/18834636/random-word-generator-python

word_site = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"


response = requests.get(word_site)
WORDS = response.content.splitlines()
WORDS = [' '.join(re.findall("[a-zA-Z]+", str(w, 'utf-8'))) for w in WORDS]
SUPPORTED_TYPE = ['varchar', 'int', 'decimal', 'date']


def random_int(lower=-10000, upper=10000, signed=True):
    """Random integer"""
    if not signed:
        return random.randint(0, upper)
    return random.randint(lower, upper)


def random_varchar(length=10, superrandom=False):
    """Random varchar strings
    I personally capped string length to 20, no matter what args the user
    provides
    if superrandom, generate from scratch
    else sample from a dictionary
    """
    if superrandom:
        return ''.join([random.choice(string.ascii_lowercase + ' ')
                        for i in range(random.randint(1, min(length, 20)))])
    else:
        w = ''
        num = int(length/20) + 1
        num = 2
        for i in range(num):
            w += ' ' + random.choice(WORDS)
        return w[:min(len(w), length)]


def random_decimal(total_dig=8, right_dig=6, signed=True):
    """Random decimal floats
    Parameters:
        total_dig (int): total number of digits to the left and right
            of the decimal point
        right_dig (int): total number of digits to the right of the
            decimal point
    Return:
        A random float
    """
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
    if not signed:
        left = random_int(lower=0, upper=left_range)
    else:
        left = random_int(lower=-left_range, upper=left_range)
    return float('.'.join([str(left), str(right)]))


def random_date():
    """Random date"""
    return "{}-{}-{}".format(random.randint(1900, 2025),
                             random.randint(1, 12),
                             random.randint(1, 28))


def random_list(datatype='int', args=None, signed=True, length=50):
    """Random list of given data type
    Parameters
       datatype (str): data type, options are ['int', 'varchar', 'decimal', 'date']
       args (list): arguments for this data type
       signed (bool): whether the number should be signed.
          Only valid for int and decimal
       length (int): length of the list
    Returns
       A list of given type
    """
    dtype = datatype.lower()
    if dtype == 'int':
        return [random_int(signed=signed) for i in range(length)]
    elif dtype == 'varchar':
        if len(args) != 1:
            raise ValueError("Invalid arguments ({}) for varchar".format(args))
        return [random_varchar(length=args[0]) for i in range(length)]
    elif dtype == 'decimal':
        if len(args) != 2:
            raise ValueError("Invalid arguments ({}) for varchar".format(args))
        return [random_decimal(total_dig=args[0],
                               right_dig=args[1],
                               signed=signed) for i in range(length)]
    elif dtype == 'date' or dtype == 'datetime':
        return [random_date() for i in range(length)]
    else:
        raise ValueError("Data type {} currently not supported".format(datatype))


def gen_null(entry, threshold=0.8):
    """Randomly return null or the input as is"""
    if random.random() > threshold:
        return ''
    return entry
